use std::fs::File;
use std::io::{self, BufRead, Read, Write};
use std::path::Path;
use memmap2::Mmap;
use std::collections::{HashMap, BTreeMap};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::thread;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use serde::Deserialize;


use crate::session_resources::node::NodeRoleType;
use crate::session_resources::datastore::{infer_type, ColumnData, ColumnType};

use super::exceptions::ClusterExceptions;

#[derive(Debug, Clone)]
pub struct FileSystemManager {
    pub accessibility_type: FileSystemType,
    pub files: HashMap<usize, FileSystemObject>
}

#[derive(Deserialize, Debug, Clone)]
pub enum FileSystemType {
    Replicated,
    Local
}

#[derive(Debug, Clone)]
pub struct FileSystemObject {
    pub path: String,
    pub partition_ordinals: BTreeMap<String, (usize, usize)>
}

impl FileSystemObject {
    pub fn to_string_for(&self, attribute: &str) -> Option<String> {
        match attribute {
            "path" => Some(self.path.clone()),
            "partition_ordinals" => Some(format!("{:?}", self.partition_ordinals)),
            _ => None,
        }
    }
}


impl fmt::Display for FileSystemType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FileSystemType::Replicated { .. } => write!(f, "Replicated"),
            FileSystemType::Local { .. } => write!(f, "Local"),
        }
    }
}

pub fn get_file(file_path: String) -> Result<File, ClusterExceptions> {
    let file: File = File::open(file_path.clone() )?;
    Ok(file)
}

pub fn get_mmap(file_path: String) -> Result<Mmap, ClusterExceptions> {
    let file: File = File::open(file_path.clone() )?;
    let mmap = unsafe {  Mmap::map(&file)? };
    Ok(mmap)
}

pub fn read_mmap_bytes(file_path: String, file_handler: Mmap, schema: HashMap<String, ColumnType>, row_terminator: u8, column_delimiter: u8) -> Result<HashMap<String, Vec<String>>, ClusterExceptions> {
    let mut bytes = HashMap::new();
    let mut columns = HashMap::new();

    let buffer = &file_handler[..];

    for (iter, line) in buffer.split(|delimiter| *delimiter == row_terminator).into_iter().enumerate() {
        for (idx, value) in line.split(|delimiter| *delimiter == column_delimiter).into_iter().enumerate() {
            if let Ok(asci_line) = String::from_utf8(value.to_vec()) {
                if iter == 1 {

                    if schema.contains_key(&asci_line) {
                        bytes.insert(asci_line.clone(), vec![]);
                        columns.insert(idx, asci_line.clone());
                    }
                }
                else {
                    if let Some(column) = columns.get_key_value(&idx) {
                        if let Some(column_data) = bytes.get_mut(column.1) {
                            column_data.push(asci_line);
                        }
                    }
                    
                }
            } else {
                return Err(ClusterExceptions::FileSystemError(FileSystemExceptions::CSVReadParseError { error_message: file_path.to_string() }));
            }
        }
    }

    Ok(bytes)
}


pub fn read_bytes_from_file(file_path: String, expected_schema: HashMap<String, ColumnType>, df_name: String) -> Self {

    let mut data: HashMap<String, ColumnData> = HashMap::new();
    let mmap = get_mmap(file_path)?;
    let raw_data = read_mmap_bytes(file_path, mmap, expected_schema, 10, 50)?;
    
}

impl FileSystemManager {

    pub fn new(accessibility: &FileSystemType) -> Self {
        Self {
            accessibility_type: accessibility.clone(),
            files: HashMap::new()
        }
    }

    pub fn get_byte_ordinals(&self, file_path: String, nodes: &Vec<String>) -> Result<BTreeMap<String, (usize, usize)>, ClusterExceptions> {

        let n_threads = nodes.len();

        // Create file object and reader
        let mmap = get_mmap(file_path.clone())?;
    
        // _1__ Determine number of lines in file
        let no_crlf = Arc::new(Mutex::new(0_usize));
        let total_bytes = mmap[..].len();
        let modulo: usize = total_bytes % n_threads;
        let chunk_size: usize = (total_bytes - modulo) / n_threads;
        let mut mmap_positions: Arc<Mutex<BTreeMap<String, (usize, usize)>>> = Arc::new(Mutex::new(BTreeMap::new()));
        let nodes = Arc::new(nodes);
    
        thread::scope(|s| {
            let mut start_pos: usize = 0;
            let mut end_pos: usize = 0;

            for n in 0..n_threads {
                // Count + Byte position of last terminator in byte block consumed by thread
                let mut mmap_positions_temp: Arc<Mutex<BTreeMap<String, (usize, usize)>>> = Arc::clone(&mmap_positions);
                let line_count = Arc::clone(&no_crlf);
                let nodes_clone = Arc::clone(&nodes);
            
                if n > 0 {
                start_pos = chunk_size * n + 1;
                }
            
                if n == (n_threads - 1) {
                end_pos += chunk_size + modulo;
                }
                else {
                end_pos += chunk_size;
                }
        
                let thread_mmap = &mmap[start_pos..end_pos];
                
                s.spawn(move || {
        
                    let mut thread_index = start_pos;
                    let mut thread_index_start = start_pos;
            
                    for byte in &thread_mmap[..] {
                        
                        if *byte == 10 {
                            *line_count.lock().unwrap() += 1;
                
                            mmap_positions_temp.lock()
                            .unwrap()
                            .entry(nodes_clone[n].to_string())
                            .and_modify(|min| min.0 += 1)
                            .and_modify(|max| if max.1 < thread_index  { *&mut max.1 = thread_index })
                            .or_insert((1, thread_index));
                        }
                        thread_index += 1;
                    }
                });
            }
        });

        let stamped_byte_ordinals = mmap_positions.lock();

        if let Ok(mmap_return) = stamped_byte_ordinals {
            Ok(mmap_return.clone())
        } else {
            Err(ClusterExceptions::FileSystemError(FileSystemExceptions::FailedToMapFile { error_message: file_path }))
        }

    }

    // For now, we will test on CSV type with | delimiter
    pub fn get_file_header(file_path: String) -> Result<HashMap<String, ColumnType>, ClusterExceptions> {

        let mmap = get_mmap(file_path.clone())?;
        let mut schema_map: HashMap<String, ColumnType> = HashMap::new();

        let mut buffer: Vec<u8> = Vec::new();
        let mut columns: Vec<Vec<String>> = Vec::new();
        let mut sample_counter = 0_usize;

        for byte in mmap.iter() {
            while sample_counter < 50 {
                buffer.push(*byte);
                if *byte == 10 {
                    sample_counter += 1;
                }
            }
        }

        for (iter, line) in buffer.split(|delimiter| *delimiter == 10).into_iter().enumerate() {
            for (idx, value) in line.split(|delimiter| *delimiter == 50).into_iter().enumerate() {
                if let Ok(asci_line) = String::from_utf8(value.to_vec()) {
                    if iter == 1 {
                        schema_map.insert(asci_line, ColumnType::Default);
                        columns.push(vec![]);
                    }
                    else {
                        columns[idx].push(asci_line);
                    }
                } else {
                    return Err(ClusterExceptions::FileSystemError(FileSystemExceptions::CSVReadParseError { error_message: file_path }));
                }
            }
        }

        for (idx, column) in schema_map.clone().keys().enumerate() {
            let sample_data = &columns[idx];

            let column_schema: ColumnType = infer_type(&sample_data);

            if let Some(column_type) = schema_map.get_mut(&column.to_string()) {
                *column_type = column_schema;
            }
        }

        Ok(schema_map)

    }

    pub fn read_from_file(&mut self, file_path: String, nodes: &Vec<String>) -> Result<usize, ClusterExceptions> {
        if let Ok(file) = self.get_byte_ordinals(file_path.clone(), nodes) {
            let mut hasher = DefaultHasher::new();
            hasher.write(file_path.as_bytes());
            let file_hash =  hasher.finish() as usize;

            let new_file_object = FileSystemObject { path: file_path.clone(), partition_ordinals: file };

            if let Some(file_existed) = self.files.insert(file_hash, new_file_object) {
                Err(ClusterExceptions::FileSystemError(FileSystemExceptions::FailedToMapFile { error_message: file_path }))
            }
            else {
                Ok(file_hash)
            }
        } else {
            Err(ClusterExceptions::FileSystemError(FileSystemExceptions::FailedToMapFile { error_message: file_path }))
        }

    }

}

#[derive(Debug, Clone)]
pub enum FileSystemExceptions {
    FailedToOpenFile { error_message: String },
    FailedToReadFromFile { error_message: String },
    FailedToMapFile { error_message: String },
    FileReadFailed { error_message: String },
    CSVReadParseError { error_message: String },
    FailedToInferFileSchema { error_message: String },
}


impl fmt::Display for FileSystemExceptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FileSystemExceptions::FailedToOpenFile { error_message} => write!(f, "Failed to open file at path '{}'.", error_message),
            FileSystemExceptions::FailedToReadFromFile { error_message} => write!(f, "Failed to read data from file at path '{}'.", error_message),
            FileSystemExceptions::FailedToMapFile { error_message} => write!(f, "Failed to map file at path '{}'.", error_message),
            FileSystemExceptions::FileReadFailed { error_message} => write!(f, "Failed to map file at path '{}' - it has already been read.", error_message),
            FileSystemExceptions::CSVReadParseError { error_message} => write!(f, "Failed to parse data in CSV format at path '{}'.", error_message),
            FileSystemExceptions::FailedToInferFileSchema { error_message} => write!(f, "Failed to infer schema of file at path '{}'.", error_message),
        }
    }
}
use std::fs::File;
use std::io::{self, BufRead, Read, Write};
use memmap2::Mmap;
use std::collections::{HashMap, BTreeMap};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::thread;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use serde::Deserialize;

use crate::session_resources::datastore::{DataFrame, Column, ColumnType};
use crate::session_resources::datastore::DatastoreExceptions;

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

pub fn read_mmap_bytes(file_path: String, file_handler: &[u8], row_terminator: u8, column_delimiter: u8) -> Result<HashMap<String, Vec<String>>, ClusterExceptions> {
    // Used to store the actual values relating to each column: Col_1: [Val_1, Val_2, Val_3]
    let mut bytes: HashMap<String, Vec<String>> = HashMap::new();

    // Used to store index position of value for each column: 0: Col_1, 1: Col_2, 2: Col_3
    let mut columns = HashMap::new();

    let buffer = &file_handler[..];

    // Split buffer by row terminator
    for (iter, line) in buffer.split(|delimiter| *delimiter == row_terminator).into_iter().enumerate() {

        // Split line by column delimiter
        for (idx, value) in line.split(|delimiter| *delimiter == column_delimiter).into_iter().enumerate() {

            // Convert ASCII bytes to string
            if let Ok(ascii_line) = String::from_utf8(value.to_vec()) {
                if iter == 1 {
                    bytes.insert(ascii_line.clone(), vec![]);
                    columns.insert(idx, ascii_line.clone());
                }
                else {
                    if let Some(column) = columns.get_key_value(&idx) {
                        if let Some(column_data) = bytes.get_mut(column.1) {
                            column_data.push(ascii_line);
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

pub fn read_csv(file_path: String, byte_ordinals: String) -> Result<DataFrame, ClusterExceptions> {

    // Create file object and reader
    let mmap = get_mmap(file_path.clone())?;
    let bytes: (usize, usize) = serde_json::from_str(byte_ordinals.as_str())?;
    let bytes_mmap = &mmap[bytes.0..bytes.1];

    let raw_data = read_mmap_bytes(file_path.clone(), bytes_mmap, 10, 50)?;
    if let Some(data_inferred_types) = DataFrame::infer_type(raw_data) {

        let df = DataFrame::new(Some(data_inferred_types));

        Ok(df)

    } else {
        return Err(ClusterExceptions::DatastoreError(DatastoreExceptions::DataTypeInferenceFailed { error_message: file_path.clone() } ))
    }

}

impl FileSystemManager {

    pub fn new(accessibility: &FileSystemType) -> Self {
        Self {
            accessibility_type: accessibility.clone(),
            files: HashMap::new()
        }
    }

    pub fn get_byte_ordinals(file_path: String, nodes: &Vec<String>) -> Result<BTreeMap<String, (usize, usize)>, ClusterExceptions> {

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
        let row_terminator = 10;
        let column_delimiter = 50;

        let mmap = get_mmap(file_path.clone())?;
        let mut columns: HashMap<usize, String> = HashMap::new();

        let mut buffer: Vec<u8> = Vec::new();
        let mut bytes: HashMap<String, Vec<String>> = HashMap::new();
        let mut sample_counter = 0_usize;

        for byte in mmap.iter() {
            while sample_counter < 50 {
                buffer.push(*byte);
                if *byte == 10 {
                    sample_counter += 1;
                }
            }
        }

        for (iter, line) in buffer.split(|delimiter| *delimiter == row_terminator).into_iter().enumerate() {

            // Split line by column delimiter
            for (idx, value) in line.split(|delimiter| *delimiter == column_delimiter).into_iter().enumerate() {
    
                // Convert ASCII bytes to string
                if let Ok(ascii_line) = String::from_utf8(value.to_vec()) {
                    if iter == 1 {
                        bytes.insert(ascii_line.clone(), vec![]);
                        columns.insert(idx, ascii_line.clone());
                    }
                    else {
                        if let Some(column) = columns.get_key_value(&idx) {
                            if let Some(column_data) = bytes.get_mut(column.1) {
                                column_data.push(ascii_line);
                            }
                        }
                        
                    }
                }
            }
        }

        let mut column_schema_return: HashMap<String, ColumnType> = HashMap::new();
        if let Some(column_schema) = DataFrame::infer_type(bytes) {

            for (column_name, column_type) in column_schema {
                let column_variant = column_type.column_type();
                column_schema_return.insert(column_name, column_variant);
            }
    
            Ok(column_schema_return)

        } else {
            return Err(ClusterExceptions::DatastoreError(DatastoreExceptions::DataTypeInferenceFailed { error_message: file_path.clone() } ));
        }

    }

    pub fn read_from_file(&mut self, file_path: String, nodes: &Vec<String>) -> Result<usize, ClusterExceptions> {
        if let Ok(file) = FileSystemManager::get_byte_ordinals(file_path.clone(), nodes) {
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
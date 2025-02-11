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
use crate::session_resources::cluster::Cluster;

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

pub fn read_mmap_bytes(file_handler: &[u8], row_terminator: u8, column_delimiter: u8) -> Result<HashMap<String, Vec<String>>, ClusterExceptions> {
    let buffer = &file_handler[..];
    Ok(structure_bytes(buffer, row_terminator, column_delimiter))
}

pub fn read_csv(node: &String, file_path: String, byte_ordinals: String, delimiter: Option<u8>) -> Result<DataFrame, ClusterExceptions> {
    const LINE_TERMINATOR: u8 = 10;

    // Create file object and reader
    let mmap = get_mmap(file_path.clone())?;
    let get_byte_ordinals: HashMap<String, [u8;2]> = serde_json::from_str(byte_ordinals.as_str())?;
    let get_byte_positions = get_byte_ordinals.get_key_value(node);

    if let Some(byte_positions) = get_byte_positions {
        let bytes = byte_positions.1;
        let start_position = bytes[0] as usize;
        let end_position = bytes[1] as usize;
        let bytes_mmap = &mmap[start_position..end_position];

        let separator: u8;
    
        if let Some(delim) = delimiter {
            separator = delim;
        } else {
            // Default to , if no delimiter supplied in function call
            separator = 44 as u8;
        }
    
        let raw_data = read_mmap_bytes(bytes_mmap, LINE_TERMINATOR, separator)?;
        let df = DataFrame::new(Some(raw_data));
    
        Ok(df)

    } else {
        return Err(ClusterExceptions::FileSystemError(FileSystemExceptions::FailedToMapFile { error_message: byte_ordinals }))
    }

}

pub fn structure_bytes(data: &[u8], row_terminator: u8, column_delimiter: u8) -> HashMap<String, Vec<String>> {
    // Used to store the actual values relating to each column: Col_1: [Val_1, Val_2, Val_3]
    let mut bytes: HashMap<String, Vec<String>> = HashMap::new();

    // Used to store index position of value for each column: 0: Col_1, 1: Col_2, 2: Col_3
    let mut columns = HashMap::new();

    // Split buffer by row terminator
    for (iter, line) in data.split(|delimiter| *delimiter == row_terminator).into_iter().enumerate() {

        // Split line by column delimiter
        for (idx, value) in line.split(|delimiter| *delimiter == column_delimiter).into_iter().enumerate() {

            // Convert ASCII bytes to string
            if let Ok(ascii_line) = String::from_utf8(value.to_vec()) {
                if iter == 0 {
                    bytes.insert(ascii_line.clone().trim_ascii().to_string(), vec![]);
                    columns.insert(idx, ascii_line.clone().trim_ascii().to_string());
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

    bytes
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
        const ROW_TERMINATOR: u8 = 10;

        // Create file object and reader
        let mmap = get_mmap(file_path.clone())?;

        // _1__ Determine number of lines in file
        let no_crlf = Arc::new(Mutex::new(0_usize));
        let total_bytes = mmap[..].len();
        let modulo: usize = total_bytes % n_threads;
        let chunk_size: usize = (total_bytes - modulo) / n_threads;
        let mmap_positions: Arc<Mutex<BTreeMap<String, (usize, usize)>>> = Arc::new(Mutex::new(BTreeMap::new()));
        let nodes = Arc::new(nodes);
    
        thread::scope(|s| {
            let mut start_pos: usize = 0;
            let mut end_pos: usize = 0;

            for n in 0..n_threads {
                // Count + Byte position of last terminator in byte block consumed by thread
                let mmap_positions_temp: Arc<Mutex<BTreeMap<String, (usize, usize)>>> = Arc::clone(&mmap_positions);
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
            
                    for byte in &thread_mmap[..] {
                        
                        if *byte == ROW_TERMINATOR {
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

        let mut next_block_start_position: usize = 0;
        let mut final_mmap_positions: BTreeMap<String, (usize, usize)> = BTreeMap::new();
        
        for (n, (node, positions)) in stamped_byte_ordinals.unwrap().clone().into_iter().enumerate() {
        
            match n {
            0 => {
                final_mmap_positions.insert(node, (0, positions.1));
            },
            x if x > 0 => {
            final_mmap_positions.insert(node, (next_block_start_position, positions.1));
            },
            _ => {
            eprintln!("Eish...something went wrong looping through mmap_positions!")
            }
            }
            
            next_block_start_position = positions.1 + 1;
        
        }

        Ok(final_mmap_positions)

    }

    pub fn get_file_header(file_path: String, column_delimiter: u8) -> Result<HashMap<String, ColumnType>, ClusterExceptions> {
        const ROW_TERMINATOR: u8 = 10;

        let mmap = get_mmap(file_path.clone())?;
        let mut buffer: Vec<u8> = Vec::new();
        let mut sample_counter = 0_usize;

        for byte in mmap.iter() {
            if sample_counter < 50 {
                buffer.push(*byte);
            }

            if *byte == 10 {
                sample_counter += 1;
            }
        }

        let bytes: HashMap<String, Vec<String>> = structure_bytes(&buffer[..], ROW_TERMINATOR, column_delimiter);

        let mut column_schema_return: HashMap<String, ColumnType> = HashMap::new();
        let column_schema = DataFrame::infer_type(bytes);

        for (column_name, column_type) in column_schema {
            let column_variant = column_type.column_type();
            column_schema_return.insert(column_name, column_variant);
        }

        Ok(column_schema_return)


    }

    pub fn read_from_file(&mut self, file_path: String, nodes: &Vec<String>) -> Result<usize, ClusterExceptions> {
        if let Ok(file) = FileSystemManager::get_byte_ordinals(file_path.clone(), nodes) {
            let mut hasher = DefaultHasher::new();
            hasher.write(file_path.as_bytes());
            let file_hash =  hasher.finish() as usize;

            let new_file_object = FileSystemObject { path: file_path.clone(), partition_ordinals: file };

            if let Some(_file_existed) = self.files.insert(file_hash, new_file_object) {
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
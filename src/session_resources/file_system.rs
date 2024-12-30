use std::fs::File;
use std::io::{self, Write, Read};
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

// Configuration for the distributed file system
const DFS_URL: &str = "http://dfs-cluster:50070/webhdfs/v1";

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
        let file: File = File::open(file_path.clone() )?;
        let mmap = unsafe {  Mmap::map(&file)? };
    
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
    pub fn get_file_header(file_path: String) -> Result<HashMap<String, String>, ClusterExceptions> {

        let file: File = File::open(file_path.clone() )?;
        let mmap = unsafe {  Mmap::map(&file)? };

        let mut buffer: Vec<u8> = Vec::new();
        let mut sample_counter = 50_usize;

        for byte in mmap.iter() {

            while sample_counter < 50 {
                buffer.push(*byte);
                if *byte == 10 {
                    sample_counter += 1;
                }
            }
 
        }

        Ok(HashMap::new())

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

    // Here we need to establish HTTP connection between cluster and node to receive bytes and write to local storage
    // Steps:
    // Create local working dir
    // Establish HTTP connection on specific port
    // Create file
    // Open file
    // Stream bytes from socket into file
    // Commence file reading into datastore
    // Fetches data from the distributed file system
    // fn fetch_from_dfs(file_path: &str, local_temp_path: &str, accessibility: FileSystemType) -> io::Result<()> {
    //     let client = Client::new();
    //     let dfs_url = format!("{DFS_URL}{file_path}?op=OPEN");

    //     println!("Fetching data from DFS: {dfs_url}");
    //     let response = client.get(&dfs_url).send().unwrap();

    //     if response.status().is_success() {
    //         let mut file = File::create(local_temp_path)?;
    //         file.write_all(&response.bytes().unwrap())?;
    //         println!("Data saved locally to {}", local_temp_path);
    //         Ok(())
    //     } else {
    //         Err(io::Error::new(
    //             io::ErrorKind::Other,
    //             "Failed to fetch data from DFS",
    //         ))
    //     }
    // }

    // /// Processes the data locally (simple word count in this example)
    // fn process_data(local_file_path: &str) -> io::Result<u32> {
    //     let mut file = File::open(local_file_path)?;
    //     let mut contents = String::new();
    //     file.read_to_string(&mut contents)?;
        
    //     // Perform a word count
    //     let word_count = contents.split_whitespace().count() as u32;
    //     println!("Word count: {}", word_count);

    //     Ok(word_count)
    // }

    // /// Writes the result back to the distributed file system
    // fn write_to_dfs(file_path: &str, data: &str) -> io::Result<()> {
    //     let client = Client::new();
    //     let dfs_url = format!("{DFS_URL}{file_path}?op=CREATE&overwrite=true");

    //     println!("Writing result to DFS: {dfs_url}");
    //     let response = client.put(&dfs_url).body(data.to_string()).send().unwrap();

    //     if response.status().is_success() {
    //         println!("Result successfully written to DFS");
    //         Ok(())
    //     } else {
    //         Err(io::Error::new(
    //             io::ErrorKind::Other,
    //             "Failed to write data to DFS",
    //         ))
    //     }
    // }

}

#[derive(Debug, Clone)]
pub enum FileSystemExceptions {
    FailedToOpenFile { error_message: String },
    FailedToReadFromFile { error_message: String },
    FailedToMapFile { error_message: String },
    FileReadFailed { error_message: String },
}


impl fmt::Display for FileSystemExceptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FileSystemExceptions::FailedToOpenFile { error_message} => write!(f, "Failed to open file at path '{}'.", error_message),
            FileSystemExceptions::FailedToReadFromFile { error_message} => write!(f, "Failed to read data from file at path '{}'.", error_message),
            FileSystemExceptions::FailedToMapFile { error_message} => write!(f, "Failed to map file at path '{}'.", error_message),
            FileSystemExceptions::FileReadFailed { error_message} => write!(f, "Failed to map file at path '{}' - it has already been read.", error_message),
        }
    }
}
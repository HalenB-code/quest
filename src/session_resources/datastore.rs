use std::{
    collections::HashMap,
    hash::Hash,
    sync::{Arc},
};
use std::fmt::Debug;
use std::any::Any;
use std::fmt;
use tokio::sync::Mutex;

use crate::session_resources::exceptions::ClusterExceptions;
use crate::session_resources::message::MessageExceptions;

//
// Cluster Objects
//

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub enum ClusterDataStoreTypes {
    DataFrame,
    KeyValue,
    Vector,
}

pub trait ClusterDataStoreTrait: Any + Send + Sync + Debug {
    fn as_any(&self) -> &dyn std::any::Any; // For type downcasting if needed
    fn clone_box(&self) -> Box<dyn ClusterDataStoreTrait>;
}

impl fmt::Display for ClusterDataStoreTypes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClusterDataStoreTypes::DataFrame { .. } => write!(f, "DataFrame"),
            ClusterDataStoreTypes::KeyValue { .. } => write!(f, "KeyValue"),
            ClusterDataStoreTypes::Vector { .. } => write!(f, "Vector"),
        }
    }
}

impl<T> ClusterDataStoreTrait for ClusterDataStoreObjects<T>
where
    T: Clone + Eq + Hash + Send + Sync + 'static + Debug,
{
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn clone_box(&self) -> Box<dyn ClusterDataStoreTrait> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn ClusterDataStoreTrait> {
    fn clone(&self) -> Box<dyn ClusterDataStoreTrait> {
        self.clone_box()
    }
}


#[derive(Debug, Clone)]
pub enum ClusterDataStoreObjects<T>
where
    T: Clone + Eq + Hash + Send + Sync + 'static + Debug,
{
    DataFrame(ClusterDataFrame),
    KeyValue(ClusterKeyValue<T>),
    Vector(ClusterVector<T>)
}

#[derive(Debug, Clone)]
pub struct ClusterDataFrame {
}

#[derive(Debug, Clone)]
pub struct ClusterKeyValue<T> {
    pub data: Arc<Mutex<HashMap<String, HashMap<String, Vec<T>>>>>,
}

impl<T> ClusterKeyValue<T>
where
    T: Clone + Eq + Hash + Send + Sync + 'static + Debug + ToString,
{
    pub fn create() -> Self {          
        Self {
            data: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClusterVector<T> {
    pub data: Arc<Mutex<Vec<T>>>
}

impl<T> ClusterVector<T>
where
    T: Clone + Eq + Hash + Send + Sync + 'static + Debug + ToString,
{
    pub fn create() -> Self {
        Self {
                data: Arc::new(Mutex::new(Vec::new())),
            }
    }

    pub async fn dump(&self) -> Option<String> {

        let mut return_string = String::new();
        let data_lock = self.data.lock().await;

        for value in data_lock.iter() {
            let string_value = value.to_string();
            string_value.clone_into(&mut return_string);
        }
        Some(return_string)
    }

}

//
// Node objects
//

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub enum NodeDataStoreTypes {
    DataFrame,
    KeyValue,
    Vector,
}

pub trait NodeDataStoreTrait: Any + Send + Sync + Debug {
    fn as_any(&self) -> &dyn std::any::Any; // For type downcasting if needed
    fn clone_box(&self) -> Box<dyn NodeDataStoreTrait>;
}

impl fmt::Display for NodeDataStoreTypes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeDataStoreTypes::DataFrame { .. } => write!(f, "DataFrame"),
            NodeDataStoreTypes::KeyValue { .. } => write!(f, "KeyValue"),
            NodeDataStoreTypes::Vector { .. } => write!(f, "Vector"),
        }
    }
}

impl<T> NodeDataStoreTrait for NodeDataStoreObjects<T>
where
    T: Clone + Eq + Hash + Send + Sync + 'static + Debug,
{
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn clone_box(&self) -> Box<dyn NodeDataStoreTrait> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn NodeDataStoreTrait> {
    fn clone(&self) -> Box<dyn NodeDataStoreTrait> {
        self.clone_box()
    }
}


#[derive(Debug, Clone)]
pub enum NodeDataStoreObjects<T>
where
    T: Clone + Eq + Hash + Send + Sync + 'static + Debug,
{
    DataFrame(DataFrame),
    KeyValue(KeyValue<T>),
    Vector(Vector<T>)
}

#[derive(Debug, Clone)]
pub struct DataFrame {
}

#[derive(Debug, Clone)]
pub struct KeyValue<T> {
    pub data: HashMap<String, HashMap<String, Vec<T>>>,
}

impl<T> KeyValue<T>
where
    T: Clone + Eq + Hash + Send + Sync + 'static + Debug + ToString,
{
    pub fn create() -> Self {          
        Self {
            data: HashMap::new(),
        }
    }

    pub fn insert_offsets(&mut self, key: &String, value: T) -> Option<usize> {

        let kv_uncommitted_check = self.data.contains_key(key);

        match kv_uncommitted_check {
            true => {
                let uncommitted_log_mref = self.data.get_mut(key);

                match uncommitted_log_mref {
                    Some(uncommitted_log) => {
                        let uncommited_log_k_check = uncommitted_log.contains_key(&"uncommitted".to_string());

                        match uncommited_log_k_check {
                            true => {
                                uncommitted_log.entry("uncommitted".to_string())
                                .and_modify(|vector| vector.push(value));
                            },
                            false => {
                                uncommitted_log.entry("uncommitted".to_string())
                                .or_insert(vec![value]);
                            }
                        }
                    },
                    None => {
                        let mut uncommitted_log_hashmap = HashMap::new();

                        uncommitted_log_hashmap.entry("uncommitted".to_string())
                        .or_insert(vec![value]);

                        self.data.entry("uncommitted".to_string())
                        .or_insert(uncommitted_log_hashmap);

                    }
                }

            },
            false => {

                let mut uncommitted_log_hashmap = HashMap::new();

                uncommitted_log_hashmap.entry("uncommitted".to_string())
                .or_insert(vec![value]);

                self.data.entry(key.clone())
                .or_insert(uncommitted_log_hashmap);

                
            }
        }
        let index = self.data.get(key)?.get(&"uncommitted".to_string())?.len();
        return Some( index );
    }

    pub fn get_offsets(&self, offsets: HashMap<String, usize>) -> Option<HashMap<String, Vec<Vec<usize>>>> {
        let mut vector_container: HashMap<String, Vec<Vec<usize>>> = HashMap::new();

        if !offsets.is_empty() {
            for (key, offset) in offsets {
                let mut key_container = vec![];
                if let Some(msg_log) = self.data.get(&key) {
                    if let Some(uncommitted_log) = msg_log.get(&"uncommitted".to_string()) {
                        for msg in uncommitted_log[offset..].iter() {
                            key_container.push(vec![offset, msg.to_string().parse::<usize>().unwrap()]);
                        }
                    }
                }
                vector_container.insert(key, key_container.clone());
            }
        }
        else {
            return None;
        }

        return Some( vector_container );
    }

    pub fn commit_offsets(&mut self, offsets: HashMap<String, usize>) -> Result<(), ClusterExceptions> {

        if !offsets.is_empty() {
            for (key, offset) in offsets {
                if let Some(msg_log) = self.data.get_mut(&key) {
                    if let None = msg_log.get(&"committed".to_string()) {
                        msg_log.insert("committed".to_string(), vec![]);
                    }

                    let mut temp_vector = vec![];

                    {
                        if let Some(uncommitted_log) = msg_log.get(&"uncommitted".to_string()) {
                            temp_vector.append(&mut uncommitted_log[offset..].to_vec());
                        }
                    }

                    if let Some(committed_log) = msg_log.get_mut(&"committed".to_string()) {
                        for (index, msg) in temp_vector.into_iter().enumerate() {
                            committed_log.insert(index, msg.clone())
                        }
                    }
                }
            }
        }
        else {
            return Err(ClusterExceptions::MessageError(MessageExceptions::CommitOffsetsError));
        }

        return Ok(());

    }

    // Only returning max committed offset per key
    pub fn list_commited_offsets(&self, keys: Vec<String>) -> Option<HashMap<String, usize>> {
        let mut return_map = HashMap::new();

        for key in keys {
            if let Some(key_logs) = self.data.get(&key) {
                if let Some(committed_log) = key_logs.get(&"committed".to_string()) {
                    if let Some(last_element) = committed_log.last() {
                        return_map.insert(key, last_element.to_string().parse::<usize>().unwrap());
                    }
                    return None;
                }   
            }
        }

        return Some(return_map);
    }

}

#[derive(Debug, Clone)]
pub struct Vector<T> {
    pub data: Vec<T>
}
// TODO
// As datastore objects at the node level do not require thread sync whereas they do at the cluster level
// separate datastore objects by Node and Cluster so appropriate data structures can be set
impl<T> Vector<T>
where
    T: Clone + Eq + Hash + Send + Sync + 'static + Debug + ToString,
{
    pub fn create() -> Self {
        Self {
                data: Vec::new(),
            }
    }

    pub fn dump(&self) -> Option<String> {

        let mut return_string = String::new();

        for value in self.data.iter() {
            let string_value = value.to_string();
            string_value.clone_into(&mut return_string);
        }
        Some(return_string)
    }

}


// Define an enum to represent dynamically typed column data
#[derive(Debug)]
enum ColumnData {
    I32(Vec<i32>),
    F64(Vec<f64>),
    Bool(Vec<bool>),
    String(Vec<String>),
}

#[derive(Debug, Clone)]
enum ColumnType {
    I32,
    F64,
    Bool,
    String,
}

// Metadata for column type lookup
struct ColumnMetadata {
    name: String,
    column_type: ColumnType,
}

// Infer the column type based on a sample
fn infer_type(sample: &[String]) -> ColumnType {
    if sample.iter().all(|val| val.parse::<i32>().is_ok()) {
        ColumnType::I32
    } else if sample.iter().all(|val| val.parse::<f64>().is_ok()) {
        ColumnType::F64
    } else if sample.iter().all(|val| val == "true" || val == "false") {
        ColumnType::Bool
    } else {
        ColumnType::String
    }
}

// Allocate columns and metadata
fn allocate_columns(
    raw_data: HashMap<String, Vec<String>>,
) -> (HashMap<String, ColumnData>, HashMap<String, ColumnMetadata>) {
    let mut typed_columns: HashMap<String, ColumnData> = HashMap::new();
    let mut metadata: HashMap<String, ColumnMetadata> = HashMap::new();

    for (column_name, values) in raw_data {
        let sample = &values[0..usize::min(10, values.len())]; // Sample first 10 values
        let column_type = infer_type(sample);

        match &column_type {
            ColumnType::I32 => {
                let typed_values: Vec<i32> = values.iter().map(|val| val.parse::<i32>().unwrap()).collect();
                typed_columns.insert(column_name.clone(), ColumnData::I32(typed_values));
            }
            ColumnType::F64 => {
                let typed_values: Vec<f64> = values.iter().map(|val| val.parse::<f64>().unwrap()).collect();
                typed_columns.insert(column_name.clone(), ColumnData::F64(typed_values));
            }
            ColumnType::Bool => {
                let typed_values: Vec<bool> = values.iter().map(|val| val == "true").collect();
                typed_columns.insert(column_name.clone(), ColumnData::Bool(typed_values));
            }
            ColumnType::String => {
                typed_columns.insert(column_name.clone(), ColumnData::String(values));
            }
        }

        metadata.insert(
            column_name.clone(),
            ColumnMetadata {
                name: column_name,
                column_type,
            },
        );
    }

    (typed_columns, metadata)
}

// Retrieve column data dynamically
fn get_column<'a>(column_name: &str, typed_columns: &'a HashMap<String, ColumnData>) -> Option<&'a ColumnData> {
    typed_columns.get(column_name)
}


// pub fn csv_reader(line: &[u8], first_line_is_header: bool) -> Option<Vec<String> {

// }
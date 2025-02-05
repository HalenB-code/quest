use std::{
    collections::HashMap,
    hash::Hash,
    sync::Arc, vec,
};
use std::fmt::Debug;
use std::any::Any;
use std::fmt;
use tokio::sync::Mutex;
use prettytable::{Table, Row, Cell};

use crate::session_resources::exceptions::ClusterExceptions;
use crate::session_resources::message::MessageExceptions;
use crate::session_resources::file_system::read_mmap_bytes;

use super::file_system::get_mmap;

//
// Node objects
//

impl fmt::Display for DataFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataFrame { .. } => write!(f, "DataFrame"),
        }
    }
}


#[derive(Debug, Clone)]
pub struct DataFrame {
    pub columns: HashMap<String, Column>,
}

impl DataFrame {

    pub fn new<T>(data: Option<HashMap<String, T>>) -> Self 
    where
    T: Into<Column>
    {
        let mut df = DataFrame {
            columns: HashMap::new(),
        };

        if let Some(data) = data {
            let inferred_data = Self.infer_type(data);

            for (col_name, column_values) in inferred_data {
                df.columns.insert(col_name, column_values);
            }
        }

        df
    }

    // Function to infer type from a sample
    fn infer_type<T>(data: HashMap<String, T>) -> Option<HashMap<String, Column>>
    where
    T: Into<Column>
    {
        let mut columns: HashMap<String, Column> = HashMap::new();

        for (key, value) in data {
            let column_value: Column = value.into();
            columns.insert(key, column_value);
        }

        Some(columns)

    }

    pub fn add_column(&mut self, name: &str, col: Column) {
        self.columns.insert(name.to_string(), col);
    }

    /// Append a row with `append_new` switch
    pub fn append_row<T>(&mut self, new_data: HashMap<String, T>, append_new: bool)
    where
        T: Into<Column>,
    {
        for (col_name, values) in new_data {
            let column_value: Column = values.into();
            self.append_value(&col_name, column_value, append_new);
        }
    }
    fn append_value(&mut self, col_name: &str, value: Column, append_new: bool) {
        match self.columns.get_mut(col_name) {
            Some(existing_col) => match (existing_col, value) {
                (Column::IntVec(existing), Column::IntVec(mut new)) => {
                    if append_new {
                        existing.push(new.pop().unwrap_or_default());
                    } else if let Some(last) = existing.last_mut() {
                        *last = new[0];
                    }
                }
                (Column::FloatVec(existing), Column::FloatVec(mut new)) => {
                    if append_new {
                        existing.push(new.pop().unwrap_or_default());
                    } else if let Some(last) = existing.last_mut() {
                        *last = new[0];
                    }
                }
                (Column::StringVec(existing), Column::StringVec(mut new)) => {
                    if append_new {
                        existing.push(new.pop().unwrap_or_default());
                    } else if let Some(last) = existing.last_mut() {
                        *last = new[0].clone();
                    }
                }
                (Column::IntNestedVec(existing), Column::IntNestedVec(mut new)) => {
                    if append_new {
                        existing.push(new.pop().unwrap_or_default());
                    } else if let Some(last) = existing.last_mut() {
                        *last = new[0].clone();
                    }
                }
                (Column::FloatNestedVec(existing), Column::FloatNestedVec(mut new)) => {
                    if append_new {
                        existing.push(new.pop().unwrap_or_default());
                    } else if let Some(last) = existing.last_mut() {
                        *last = new[0].clone();
                    }
                }
                (Column::StringNestedVec(existing), Column::StringNestedVec(mut new)) => {
                    if append_new {
                        existing.push(new.pop().unwrap_or_default());
                    } else if let Some(last) = existing.last_mut() {
                        *last = new[0].clone();
                    }
                }
                _ => panic!("Column type mismatch!"),
            },
            None => {
                // Column doesn't exist, create it based on inferred type
                self.columns.insert(col_name.to_string(), value);
            }
        }
    }

    fn print_table(&self) {
        let mut table = Table::new();

        // Collect column names
        let column_names: Vec<String> = self.columns.keys().cloned().collect();
        table.add_row(Row::new(column_names.iter().map(|name| Cell::new(name)).collect()));

        // Determine max row count
        let max_rows = self.columns.values().map(|col| col.len()).max().unwrap_or(0);

        // Collect rows
        for i in 0..max_rows {
            let mut row = vec![];
            for col in &column_names {
                row.push(Cell::new(&self.columns[col].get_value_at(i)));
            }
            table.add_row(Row::new(row));
        }

        table.printstd();
    }

    fn filter<F>(&self, predicate: F) -> Vec<HashMap<String, String>>
    where
        F: Fn(&HashMap<String, String>) -> bool,
        
    {
        let max_rows = self.columns.values().map(|col| col.len()).max().unwrap_or(0);
        let mut result = Vec::new();

        for i in 0..max_rows {
            let mut row = HashMap::new();
            for (col_name, col) in &self.columns {
                row.insert(col_name.clone(), col.get_value_at(i));
            }

            if predicate(&row) {
                result.push(row);
            }
        }

        result
    }

    pub fn committ_offsets(&mut self, offsets: HashMap<String, usize>) -> Result<(), ClusterExceptions> {
        
        for (key, offset) in offsets.into_iter() {
            if let Some(column) = self.columns.get(&"Key".to_string()) {
                // Obtain the index position of the supplied key to retrieve the offsets from the corresponding Value column
                // Limiting the impl to only non-nested Column to prevent massive string dump to std out if called on nested arrays
                if let Some(key_column) = column.get_values() {
                    let column_index_position_to_update = key_column.into_iter().enumerate().filter(|(index, value)| value == &key).collect();
        
                    let filtered_data = self.filter(|row| row.get("Key").map_or(false, |v| v == &key));

                    if filtered_data.len() > 1 {
                        return Err(ClusterExceptions::DatastoreError(DatastoreExceptions::MultipleFilterReturnCommittOffsets { error_message: key.to_string() }));
                    } else {
                        if let Some(existing_offsets) = filtered_data[0].get(&"Status".to_string()) {
                            let mut offset_elements = existing_offsets.split(",").map(|element| element.parse::<usize>().unwrap()).collect::<Vec<usize>>();

                            offset_elements.iter().enumerate().map(|(element, index)| if index <= &offset { 1 } else {0} );

                            if let Some(column_mut) = self.columns.get_mut(&"Key".to_string()) {
                                column_mut.overwrite(offset_elements, column_index_position_to_update.1);
                            }
                        }
                    }

                }
            } else {
                return Err(ClusterExceptions::DatastoreError(DatastoreExceptions::MultipleFilterReturnCommittOffsets { error_message: key.to_string() }));
            }
        }

        Ok(())
        
    }

    pub fn get_offsets(&self, offsets: HashMap<String, usize>) -> Result<HashMap<String, Vec<Vec<usize>>>, ClusterExceptions> {
        let mut vector_container: HashMap<String, Vec<Vec<usize>>> = HashMap::new();

        if !offsets.is_empty() {
            for (key, offset) in offsets {

                let mut key_container = vec![];

                if let Some(column) = self.columns.get(&"Key".to_string()) {
                    let filtered_data = self.filter(|row| row.get("Key").map_or(false, |v| v == &key));
    
                    if filtered_data.len() > 1 {
                        return Err(ClusterExceptions::DatastoreError(DatastoreExceptions::MultipleFilterReturnCommittOffsets { error_message: key.to_string() }));
                    } else {
                        let existing_offsets = filtered_data[0].get(&"Status".to_string());
                        let existing_msgs = filtered_data[0].get(&"Value".to_string());

                        match (existing_offsets, existing_msgs) {
                            (Some(offsets), Some(data)) => {
                                let offsets = offsets.split(",").map(|element| element.parse::<usize>().unwrap()).collect::<Vec<usize>>();
                                let values = data.split(",").map(|element| element.parse::<usize>().unwrap()).collect::<Vec<usize>>();

                                for (offset, msg) in offsets.into_iter().zip(values) {
                                    key_container.push(vec![offset, msg]);
                                }

                                vector_container.insert(key, key_container.clone());
                            },
                            _ => {
                                return Err(ClusterExceptions::DatastoreError(DatastoreExceptions::OffsetsDoNotExist { error_message: key.to_string() }));
                            }
                        }
                    }
                }
            }
                
        } else {
                return Err(ClusterExceptions::DatastoreError(DatastoreExceptions::ColumnDoesNotExist { error_message: "Key".to_string() }));
        }

    Ok( vector_container )

    }

    pub fn list_committed_offsets(&self, keys: Vec<String>) -> Result<HashMap<String, usize>, ClusterExceptions> {
        let mut return_map = HashMap::new();

            if !keys.is_empty() {
                for key in keys {
    
                    if let Some(column) = self.columns.get(&"Key".to_string()) {
                        let filtered_data = self.filter(|row| row.get("Key").map_or(false, |v| v == &key));
        
                        if filtered_data.len() > 1 {
                            return Err(ClusterExceptions::DatastoreError(DatastoreExceptions::MultipleFilterReturnCommittOffsets { error_message: key.to_string() }));
                        } else {
                            if let Some(existing_offsets) = filtered_data[0].get(&"Status".to_string()) {
                                let offsets = existing_offsets.split(",").map(|element| element.parse::<usize>().unwrap()).collect::<Vec<usize>>();
                                let max_committed_offset = offsets.iter().filter(|element| **element == 1).collect().len();

                                return_map.insert(key, max_committed_offset);
                            }
                        }
                    }
                }
                    
            } else {
                    return Err(ClusterExceptions::DatastoreError(DatastoreExceptions::ColumnDoesNotExist { error_message: "Key".to_string() }));
            }

        Ok(return_map)

    }

    pub fn get_vector(&self, column: String) -> Option<Vec<String>> {

        if let Some(column) = self.columns.get(&column) {
            if let Some(key_column) = column.get_values() {
                return Some(key_column);
            }
        }
        None
    }



    pub fn insert_offsets(&mut self, key_value_insert: HashMap<String, String>) -> Option<usize> {

        self.append_row(key_value_insert.clone(), false);

        for (key, offset) in key_value_insert.iter() {
            if let Some(column) = self.columns.get(key) {
                if let Some(key_column) = column.get_values() {
                    return Some(key_column.len());
                }
            }
        }
        None
    }

    pub fn sum(&self, column: String) -> Option<usize> {

        if let Some(column) = self.columns.get(&column) {
            if let Some(values) = column.get_values() {
                let sum_total = values.iter().fold(0, |acc, &x| acc + x);
                return Some(sum_total);
            }
        }

        None
    }

    
}


// Define an enum to represent dynamically typed column data
#[derive(Debug, Clone)]
pub enum Column {
    IntVec(Vec<i32>),
    FloatVec(Vec<f64>),
    StringVec(Vec<String>),
    IntNestedVec(Vec<Vec<i32>>),
    FloatNestedVec(Vec<Vec<f64>>),
    StringNestedVec(Vec<Vec<String>>),
}

impl From<i32> for Column {
    fn from(value: i32) -> Self {
        Column::IntVec(vec![value])
    }
}

impl From<f64> for Column {
    fn from(value: f64) -> Self {
        Column::FloatVec(vec![value])
    }
}

impl From<String> for Column {
    fn from(value: String) -> Self {
        Column::StringVec(vec![value])
    }
}

impl From<&str> for Column {
    fn from(value: &str) -> Self {
        Column::StringVec(vec![value.to_string()])
    }
}

impl From<Vec<i32>> for Column {
    fn from(value: Vec<i32>) -> Self {
        Column::IntVec(value)
    }
}

impl From<Vec<f64>> for Column {
    fn from(value: Vec<f64>) -> Self {
        Column::FloatVec(value)
    }
}

impl From<Vec<String>> for Column {
    fn from(value: Vec<String>) -> Self {
        Column::StringVec(value)
    }
}

impl From<Vec<Vec<i32>>> for Column {
    fn from(value: Vec<Vec<i32>>) -> Self {
        Column::IntNestedVec(value)
    }
}

impl From<Vec<Vec<f64>>> for Column {
    fn from(value: Vec<Vec<f64>>) -> Self {
        Column::FloatNestedVec(value)
    }
}

impl From<Vec<Vec<String>>> for Column {
    fn from(value: Vec<Vec<String>>) -> Self {
        Column::StringNestedVec(value)
    }
}

impl Column {
    pub fn data_type(&self) -> String {
        match self {
            Column::IntVec(_) => "Int".to_string(),
            Column::FloatVec(_) => "Float".to_string(),
            Column::StringVec(_) => "String".to_string(),
            Column::IntNestedVec(_) => "IntNested".to_string(),
            Column::FloatNestedVec(_) => "FloatNested".to_string(),
            Column::StringNestedVec(_) => "StringNested".to_string(),
        }
    }

    pub fn get_value_at(&self, index: usize) -> String {
        match self {
            Column::IntVec(v) => v.get(index).map_or("".to_string(), |x| x.to_string()),
            Column::FloatVec(v) => v.get(index).map_or("".to_string(), |x| x.to_string()),
            Column::StringVec(v) => v.get(index).map_or("".to_string(), |x| x.clone()),
            Column::IntNestedVec(v) => v.get(index).map_or("".to_string(), |x| format!("{:?}", x)),
            Column::FloatNestedVec(v) => v.get(index).map_or("".to_string(), |x| format!("{:?}", x)),
            Column::StringNestedVec(v) => v.get(index).map_or("".to_string(), |x| format!("{:?}", x)),
        }
    }

    pub fn get_values(&self) -> Option<Vec<String>> {
        match self {
            Column::IntVec(v) => Some(v.iter().map(|element| element.to_string()).collect::<Vec<String>>()),
            Column::FloatVec(v) => Some(v.iter().map(|element| element.to_string()).collect::<Vec<String>>()),
            Column::StringVec(v) => Some(v.iter().map(|element| element.to_string()).collect::<Vec<String>>()),
            _ => None,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Column::IntVec(v) => v.len(),
            Column::FloatVec(v) => v.len(),
            Column::StringVec(v) => v.len(),
            Column::IntNestedVec(v) => v.len(),
            Column::FloatNestedVec(v) => v.len(),
            Column::StringNestedVec(v) => v.len(),
        }
    }

    pub fn append(&mut self, value: Column, append_new: bool) {
        match (self, value) {
            (Column::IntVec(v), Column::IntVec(mut new)) => {
                if append_new {
                    v.push(new.remove(0));
                } else if let Some(last) = v.last_mut() {
                    *last = new.remove(0);
                }
            }
            (Column::FloatVec(v), Column::FloatVec(mut new)) => {
                if append_new {
                    v.push(new.remove(0));
                } else if let Some(last) = v.last_mut() {
                    *last = new.remove(0);
                }
            }
            (Column::StringVec(v), Column::StringVec(mut new)) => {
                if append_new {
                    v.push(new.remove(0));
                } else if let Some(last) = v.last_mut() {
                    *last = new.remove(0);
                }
            }
            (Column::IntNestedVec(v), Column::IntNestedVec(mut new)) => {
                if append_new {
                    v.push(new.remove(0));
                } else if let Some(last) = v.last_mut() {
                    last.extend(new.remove(0));
                }
            }
            (Column::FloatNestedVec(v), Column::FloatNestedVec(mut new)) => {
                if append_new {
                    v.push(new.remove(0));
                } else if let Some(last) = v.last_mut() {
                    last.extend(new.remove(0));
                }
            }
            _ => panic!("Column type mismatch!"),
        }
    }

    pub fn overwrite(&mut self, value: Vec<usize>, index: usize) {
        match (self) {
            Column::IntVec(v) => {
                let value = value.iter().map().collect::<Vec<i32>();
                *v[index] = value;
            }
            Column::FloatVec(v) => {
                let value = value.iter().map().collect::<Vec<f64>();
                *v[index] = value;
            }
            Column::StringVec(v) => {
                let value = value.iter().map().collect::<Vec<String>();
                *v[index] = value;
            }
            Column::IntNestedVec(v) => {
                let value = value.iter().map().collect::<Vec<>();
                *v[index] = value;
            }
            Column::FloatNestedVec(v) => {
                let value = value.iter().map().collect::<Vec<>();
                *v[index] = value;
            }
            _ => panic!("Column type mismatch!"),
        }
    }

}

#[derive(Debug, Clone)]
pub enum DatastoreExceptions {
    CSVReaderParseError { error_message: String },
    FailedToUpdateRow { error_message: String },
    MultipleFilterReturnCommittOffsets { error_message: String },
    ColumnDoesNotExist { error_message: String },
    OffsetsDoNotExist { error_message: String },
}


impl fmt::Display for DatastoreExceptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DatastoreExceptions::CSVReaderParseError { error_message} => write!(f, "Failed to parse value '{}' when reading file.", error_message),
            DatastoreExceptions::FailedToUpdateRow { error_message} => write!(f, "Failed to update row with value '{}'.", error_message),
            DatastoreExceptions::MultipleFilterReturnCommittOffsets { error_message} => write!(f, "Multiple results were returned for offset filter predicate '{}'.", error_message),
            DatastoreExceptions::ColumnDoesNotExist { error_message} => write!(f, "Column '{}' does not exist in dataframe.", error_message),
            DatastoreExceptions::OffsetsDoNotExist { error_message} => write!(f, "Offsets do not exist for '{}'.", error_message),
        }
    }
}
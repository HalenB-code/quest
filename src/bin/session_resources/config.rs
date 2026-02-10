use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead};
use std::fmt;

use crate::session_resources::exceptions::ClusterExceptions;
use crate::session_resources::file_system::FileSystemType;
use crate::session_resources::implementation::{ImplementationMode};

#[derive(Deserialize, Debug, Clone)]
pub struct ClusterConfig {
    pub working_directory: WorkingDirectory,
    pub network: Network,
    pub cluster_settings: ClusterSettings,
}

#[derive(Deserialize, Debug, Clone)]
//#[serde(rename = "working_directory")]
pub struct WorkingDirectory {
    pub local_path: String,
    pub wal_path: String,
    pub file_system_type: FileSystemType
}

#[derive(Deserialize, Debug, Clone)]
//#[serde(rename = "working_directory")]
pub struct Network {
    pub binding_address: String,
    pub orchestrator_port: String,
    pub layout: HashMap<String, HashMap<String, String>>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ClusterSettings {
    pub execution_mode: ImplementationMode,
}

impl ClusterConfig {
    pub fn read_config(source_path: String) -> Self {

        let file = File::open(source_path).unwrap();
        let reader = io::BufReader::new(file);

        let lines: String = reader
        .lines()
        .map(|line| line.unwrap_or_default())
        .collect::<Vec<String>>()
        .join("\n");

        // let parsed_data = parse_toml_like(&lines).unwrap();

        let parsed_data: ClusterConfig = toml::from_str(&lines).unwrap();

        // Convert the map into JSON string (Serde supports JSON-like maps)
        //let json_str = serde_json::to_string(&parsed_data).unwrap();

        // Deserialize into Config
        //let config: ClusterConfig = serde_json::from_str(&json_str).unwrap();

        parsed_data

    }
}


pub fn parse_toml_like(input: &str) -> Result<HashMap<String, Value>, ClusterExceptions> {
    let mut data = HashMap::new();
    let mut current_section = None;

    for line in input.lines() {
        let line = line.trim();

        // Skip empty lines or comments
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        if line.starts_with('[') && line.ends_with(']') {
            // Parse section headers like [server] or [database]
            current_section = Some(line[1..line.len() - 1].to_string());
        } else if let Some(eq_pos) = line.find('=') {
            // Parse key-value pairs
            let key = line[..eq_pos].trim().to_string();
            let value = line[eq_pos + 1..].trim().to_string();

            if let Some(section) = &current_section {
                // Insert into a nested structure under the current section
                let section_key = section.clone();
                let section_data = data.entry(section_key).or_insert_with(|| Value::Object(serde_json::Map::new()));
                if let Value::Object(map) = section_data {
                    map.insert(key, parse_value(&value));
                }
            } else {
                // If no section, insert at the root level
                data.insert(key, parse_value(&value));
            }
        }
    }

    Ok(data)
}

pub fn parse_value(value: &str) -> Value {
    if value.starts_with('"') && value.ends_with('"') {
        // Parse strings
        Value::String(value[1..value.len() - 1].to_string())
    } else if let Ok(num) = value.parse::<i64>() {
        // Parse integers
        Value::Number(num.into())
    } else if let Ok(num) = value.parse::<f64>() {
        // Parse floats
        Value::Number(serde_json::Number::from_f64(num).unwrap())
    } else if value == "true" || value == "false" {
        // Parse booleans
        Value::Bool(value == "true")
    } else {
        // Fallback: treat as string
        Value::String(value.to_string())
    }
}

#[derive(Debug, Clone)]
pub enum ConfigExceptions {
    FailedToParseConfig { error_message: String },
}


impl fmt::Display for ConfigExceptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigExceptions::FailedToParseConfig {error_message} => write!(f, "Failed to parse the provided config file '{}'.", error_message),
        }
    }
}



use std::fmt;
use std::collections::{HashMap, BTreeMap};
use serde::{Serialize, Deserialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use serde_json;
use std::time::{SystemTime, UNIX_EPOCH};
use chrono::{DateTime, Local};

use crate::session_resources::message::{MessageFields, MessageType, MessageTypeFields, Message};
use crate::session_resources::exceptions::ClusterExceptions;
use crate::session_resources::write_ahead_log::{WalEntry, WriteAheadLog};
use crate::session_resources::cli::ClusterCommand;
use crate::session_resources::file_system::{FileSystemManager};
use crate::session_resources::message::message_deserializer;
use crate::session_resources::file_system::FileSystemType;

#[derive(Debug)]
pub struct QueryPlan {
    pub query_plan_id: usize,
    pub query_plan_steps: BTreeMap<usize, BTreeMap<usize, (String, QueryPlanTypes, Message, QueryPlanStatus)>>, // usize is step of query plan; usize is sub-step of query plan step; String = node_id, QueryPlanTypes, String = message string, QueryPlanType Status
}

#[derive(Debug, Clone)]
pub enum QueryPlanTypes {
    Aggregate,
    WriteToFile,
    ReadFromFile,
    AggregateExtend,
    DisplayDf
}

#[derive(Debug, Clone, PartialEq)]
pub enum QueryPlanStatus {
    Pending,
    Complete,
    Failed
}

fn generate_query_plan_step_id(node_id: &String) -> String {

    let alphabet_lookup: HashMap<usize, char> = HashMap::from([
        (0, 'X')
        , (1, 'a')
        , (2, 'b')
        , (3, 'c')
        , (4, 'd')
        , (5, 'e')
        , (6, 'f')
        , (7, 'g')
        , (8, 'h')
        , (9, 'i')
        , (10, 'j')
        , (11, 'k')
        , (12, 'l')
        , (13, 'm')
        , (14, 'n')
        , (15, 'p')
        , (16, 'o')
        , (17, 'q')
        , (18, 'r')
        , (19, 's')
        , (20, 't')
        , (21, 'u')
        , (22, 'v')
        , (23, 'w')
        , (24, 'x')
        , (25, 'y')
        , (26, 'z')
    ]);


    // Get the current system time
    let now = SystemTime::now();

    // Convert to DateTime<Local> for formatting
    let datetime: DateTime<Local> = now.into();

    // Format using strftime-like patterns
    let formatted_datetime = datetime.format("%Y%m%d%H%M%S%f").to_string();

    // Create uuid
    let mut uuid = String::with_capacity(26);
    
    for c in node_id.chars() {
        uuid.push(c);
    }
    uuid.push('-');

    for (i, c) in formatted_datetime.chars().enumerate() {

        if i == 0 {
                uuid.push(*alphabet_lookup.get(&c.to_string().parse::<usize>().unwrap()).unwrap());
            }
            else {
                if i % 2 == 0 {
                    uuid.push(c);
                } else {
                    uuid.push(*alphabet_lookup.get(&c.to_string().parse::<usize>().unwrap()).unwrap());
                }
            }

        }
    uuid
}
    

impl QueryPlan {

    pub fn new() -> Self {

        QueryPlan {
            query_plan_id: 0,
            query_plan_steps: BTreeMap::new(),
        }

    }

    // This creates a message type for each request in the cluster command
    // The messages are stored in the query plan steps with the node target, query plan type, message string, and status
    pub async fn add_step(&mut self, file_system_manager: &mut FileSystemManager, active_cluster_nodes: Vec<String>, cluster_command: ClusterCommand) -> Result<usize, ClusterExceptions> {

        match cluster_command {

            ClusterCommand::CmdGroupBy { target_name, aggregation_keys, aggregation_type } => {

                let query_plan_idx = self.query_plan_steps.len() + 1;
                let mut query_plan_steps = BTreeMap::new();

                // Step 1: Aggregate
                let query_plan_step_idx = query_plan_steps.len() + 1;
                // Assuming here target_name is already in node datastore
                if let Some(mut message_request_string) = Message::default_request_message("Aggregate") {
                    message_request_string.set_body(MessageType::Aggregate {
                        df_name: target_name.clone(),
                        keys: aggregation_keys.clone(),
                        agg_type: aggregation_type.clone(),
                    });
                    query_plan_steps.insert(query_plan_step_idx, (generate_query_plan_step_id(message_request_string.dest().unwrap()), QueryPlanTypes::Aggregate, message_request_string, QueryPlanStatus::Pending));

                } else {
                    return Err(ClusterExceptions::InvalidCommand { error_message: "aggregate".to_string() });
                }

                // Step 2: Write to file
                if let Some(mut message_request_string) = Message::default_request_message("WriteToFile") {
                    // Need a method to lookup at the cluster level the datastore target to retrieve the schema
                    // Otherwise a separate messaged must be sent to a node to retrieve the schema and that costs more than storing metadata in the cluster

                    message_request_string.set_body(MessageType::WriteToFile {
                        file_path: format!("{}/{}/intermediate_data_file.csv", file_system_manager.local_working_directory.clone(), target_name),
                        file_format: aggregation_type.clone(),
                        // Schema
                    });
                    let query_plan_step_idx = query_plan_steps.len() + 1;

                    query_plan_steps.insert(query_plan_step_idx, (generate_query_plan_step_id(message_request_string.dest().unwrap()), QueryPlanTypes::WriteToFile, message_request_string, QueryPlanStatus::Pending));

                } else {
                    return Err(ClusterExceptions::InvalidCommand { error_message: "write_to_file".to_string() });
                }

                // Step 3: Read from file
                if let Some(mut message_request_string) = Message::default_request_message("ReadFromFile") {
                    // Assuming the file is stored in the local file system
                    message_request_string.set_body(MessageType::ReadFromFile {
                        file_path: format!("{}/{}/intermediate_data_file.csv", file_system_manager.local_working_directory.clone(), target_name),
                        accessibility: "local".to_string(), // Assuming local for now
                        bytes: "".to_string(), // Placeholder for byte ordinals
                        schema: "".to_string(), // Placeholder for schema
                    });

                    let query_plan_step_idx = query_plan_steps.len() + 1;
                    query_plan_steps.insert(query_plan_step_idx, (generate_query_plan_step_id(message_request_string.dest().unwrap()), QueryPlanTypes::ReadFromFile, message_request_string, QueryPlanStatus::Pending));

                } else {
                    return Err(ClusterExceptions::InvalidCommand { error_message: "read-from-file".to_string() });
                }

                // Step 4: Aggregate extend

                // Union
                if let Some(mut message_request_string) = Message::default_request_message("Union") {

                    // TODO: Add Union Msg Type so AggregateExtend = 1. Union intermediate results, 2. Aggregate again
                    message_request_string.set_body(MessageType::Union {
                        df_name: target_name.clone(),
                        keys: aggregation_keys.clone(),
                    });

                    let query_plan_step_idx = query_plan_steps.len() + 1;
                    query_plan_steps.insert(query_plan_step_idx, (generate_query_plan_step_id(message_request_string.dest().unwrap()), QueryPlanTypes::AggregateExtend, message_request_string, QueryPlanStatus::Pending));

                } else {
                    return Err(ClusterExceptions::InvalidCommand { error_message: "aggregate-extend-union".to_string() });
                }

                // Aggregate
                if let Some(mut message_request_string) = Message::default_request_message("AggregateExtend") {
                    
                    // TODO: Add Union Msg Type so AggregateExtend = 1. Union intermediate results, 2. Aggregate again
                    message_request_string.set_body(MessageType::Aggregate {
                        df_name: target_name.clone(),
                        keys: aggregation_keys.clone(),
                        agg_type: aggregation_type.clone(),
                    });

                    let query_plan_step_idx = query_plan_steps.len() + 1;
                    query_plan_steps.insert(query_plan_step_idx, (generate_query_plan_step_id(message_request_string.dest().unwrap()), QueryPlanTypes::AggregateExtend, message_request_string, QueryPlanStatus::Pending));

                } else {
                    return Err(ClusterExceptions::InvalidCommand { error_message: "aggregate-extend".to_string() });
                }
                self.query_plan_steps.insert(query_plan_idx, query_plan_steps);
                return Ok(query_plan_idx);

            },
            ClusterCommand::CmdDisplayDf { target_name, target_node, n_rows } => {
                let query_plan_idx = self.query_plan_steps.len() + 1;
                let mut query_plan_steps = BTreeMap::new();
                let cluster_nodes = active_cluster_nodes.clone();

                // Step 1: Display DataFrame
                let query_plan_step_idx = query_plan_steps.len() + 1;

                if target_node != "all" {
                    if let Some(mut message_request_string) = Message::default_request_message("DisplayDf") {
                        message_request_string.set_dest(target_node.to_string());
                        message_request_string.set_body(MessageType::DisplayDf {
                            df_name: target_name.clone(),
                            total_rows: n_rows,
                        });

                        query_plan_steps.insert(query_plan_step_idx, (generate_query_plan_step_id(message_request_string.dest().unwrap()), QueryPlanTypes::DisplayDf, message_request_string, QueryPlanStatus::Pending));
                    } else {
                        return Err(ClusterExceptions::InvalidCommand { error_message: "display-df".to_string() });
                    }
                    
                } else {

                    // If target_node is "all", then we need to send the request to all nodes
                    let all_nodes = cluster_nodes;

                    for node in all_nodes.iter() {
                        if let Some(mut message_request_string) = Message::default_request_message("DisplayDf") {
                            message_request_string.set_dest(node.clone());
                            message_request_string.set_body(MessageType::DisplayDf {
                                df_name: target_name.clone(),
                                total_rows: n_rows,
                            });
                        query_plan_steps.insert(query_plan_step_idx, (generate_query_plan_step_id(message_request_string.dest().unwrap()), QueryPlanTypes::DisplayDf, message_request_string, QueryPlanStatus::Pending));
                        }
                    }
                }
                self.query_plan_steps.insert(query_plan_idx, query_plan_steps);
                return Ok(query_plan_idx);
            }, 
            ClusterCommand::CmdReadFile { target_file_path, target_node, delimiter } => {
                let query_plan_idx = self.query_plan_steps.len() + 1;
                let mut query_plan_steps = BTreeMap::new();

                let cluster_nodes = active_cluster_nodes.clone();
                let full_file_path = format!("{}\\{}", file_system_manager.local_working_directory.clone(), target_file_path);

                // TODO: Support other delimiters
                let separator = delimiter.to_string().as_bytes()[0];

                let file_system_type = file_system_manager.local_working_directory.clone();
                let infered_file_schema = FileSystemManager::get_file_header(full_file_path.clone(), separator)?;
                let infered_file_schema_string = serde_json::to_string(&infered_file_schema)?;

                // Step 1: Read from file
                let mut query_plan_step_idx = query_plan_steps.len() + 1;

                if target_node != "all" {

                    let nodes = target_node.split(',').collect::<Vec<&str>>();

                    let file_path_hash = file_system_manager.read_from_file(full_file_path.clone(), &cluster_nodes)?;
                    let byte_ordinals = FileSystemManager::get_byte_ordinals(full_file_path.clone(), &cluster_nodes)?;
                    let byte_ordinals_string = serde_json::to_string(&byte_ordinals)?;

                    for node in nodes.iter() {
                        let target_node = node.to_string();
                        if let Some(mut message_request_string) = Message::default_request_message("ReadFromFile") {
                            message_request_string.set_dest(target_node.clone());
                            message_request_string.set_body(MessageType::ReadFromFile {
                                file_path: target_file_path.clone(),
                                accessibility: "local".to_string(), // Assuming local for now
                                bytes: byte_ordinals_string.clone(),
                                schema: infered_file_schema_string.clone(),
                            });
                            query_plan_steps.insert(query_plan_step_idx, (generate_query_plan_step_id(message_request_string.dest().unwrap()), QueryPlanTypes::ReadFromFile, message_request_string, QueryPlanStatus::Pending));

                            query_plan_step_idx += 1;
                        }
                    }

                } else {

                    // If target_node is "all", then we need to send the request to all nodes
                    let all_nodes = cluster_nodes;

                    let file_path_hash = file_system_manager.read_from_file(full_file_path.clone(), &all_nodes)?;
                    let byte_ordinals = FileSystemManager::get_byte_ordinals(full_file_path.clone(), &all_nodes)?;
                    let byte_ordinals_string = serde_json::to_string(&byte_ordinals)?;

                    for node in all_nodes.iter() {
                        if let Some(mut message_request_string) = Message::default_request_message("ReadFromFile") {
                            message_request_string.set_dest(node.clone());
                            message_request_string.set_body(MessageType::ReadFromFile {
                                file_path: target_file_path.clone(),
                                accessibility: "local".to_string(), // Assuming local for now
                                bytes: byte_ordinals_string.clone(),
                                schema: infered_file_schema_string.clone(),
                            });

                            query_plan_steps.insert(query_plan_step_idx, (generate_query_plan_step_id(message_request_string.dest().unwrap()), QueryPlanTypes::ReadFromFile, message_request_string, QueryPlanStatus::Pending));

                            query_plan_step_idx += 1;
                        }
                    }

                }
                self.query_plan_steps.insert(query_plan_idx, query_plan_steps);
                return Ok(query_plan_idx);
            },
            ClusterCommand::CmdMessageString { message } => {

                let query_plan_idx = self.query_plan_steps.len() + 1;
                let mut query_plan_steps = BTreeMap::new();
                let message_encoded = message_deserializer(&message)?;

                query_plan_steps.insert(query_plan_idx, (generate_query_plan_step_id(message_encoded.dest().unwrap()), QueryPlanTypes::ReadFromFile, message_encoded.clone(), QueryPlanStatus::Pending));
                self.query_plan_steps.insert(query_plan_idx, query_plan_steps);
                return Ok(query_plan_idx);

            },
            _ => {
                Err(ClusterExceptions::InvalidCommand { error_message: "display-df".to_string() })
            }

        }

    }

    pub fn get_latest_query_step(&mut self) -> usize {
        if let Some(last_entry_id) =  self.query_plan_steps.last_key_value() {
        return *last_entry_id.0;
        } else {
            return 0;
        }
    }

    pub fn get_all_query_steps(&mut self) -> Vec<usize> {
        let mut all_steps: Vec<usize> = Vec::new();
        for (step_id, _step) in self.query_plan_steps.iter() {
            all_steps.push(*step_id);
        }
        all_steps
    }

    pub fn update_step_status(&mut self, target_step_id: Option<usize>, message_id: String, new_status: QueryPlanStatus) -> Result<(), ClusterExceptions> {

        match target_step_id {
            Some(target_step_id) => {
                for (step_id, step_details) in self.query_plan_steps.iter_mut() {
                    
                    if &target_step_id == step_id {
                        for (_sub_step_id, (query_plan_message_id, _, _, message_status)) in step_details.iter_mut() {

                            if query_plan_message_id.clone() == message_id {
                                *message_status = new_status.clone();
                            } else {
                                continue
                            }
                        };
                    }
                }
                return Ok(());
            },
            None => {
                for (step_id, step_details) in self.query_plan_steps.iter_mut() {
                    
                    for (_sub_step_id, (query_plan_message_id, _, _, message_status)) in step_details.iter_mut() {

                        if query_plan_message_id.clone() == message_id {
                            *message_status = new_status.clone();
                        } else {
                            continue
                        }
                    };
                }
                return Ok(());
            }
        }

    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub enum TransactionTypes {
    Read,
    Write,
    ReadFile,
    DisplayDf,
    GroupBy
}

#[derive(Debug, Clone, PartialEq)]
pub enum TransactionState {
    Pending,
    Committed,
    Aborted,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Action {
    pub key: TransactionTypes,
    pub value: Option<Message>, // None for deletions.
}

impl Action {
    pub fn build(action: TransactionTypes, steps: &HashMap<String, String>, message: Message, remote: Option<String>) -> Result<Self, ClusterExceptions> {

        let steps = steps.clone();

        match action {
            TransactionTypes::Read => {

                let target = steps.get(&"key".to_string()).unwrap().to_string();
                let value = steps.get(&"value".to_string()).unwrap().to_string();
                // Read is 1 action
                return Ok( Action { 
                    key: TransactionTypes::Read, 
                    value: Some( Message::Request { 
                        src: message.src().unwrap().clone(), 
                        dest: message.dest().unwrap().clone(), 
                        body: MessageType::KeyValueRead { 
                            key: target.clone() 
                        } 
                    } )
                } );
            },
            TransactionTypes::Write => {
                let target = steps.get(&"key".to_string()).unwrap().to_string();
                let value = steps.get(&"value".to_string()).unwrap().to_string();

                // Write is 2 actions: Local & Remote writes
                match remote {
                    Some(other_node) => {
                        return Ok( Action { 
                            key: TransactionTypes::Write, 
                            value: Some( Message::Request { 
                                src: message.dest().unwrap().clone(), 
                                dest: other_node, 
                                body: MessageType::KeyValueWrite { 
                                    key: target.clone(), 
                                    value: value.clone()
                                } 
                            } ) 
                        } );
                    },
                    None => {
                        return Ok( Action { 
                            key: TransactionTypes::Write, 
                            value: Some( Message::Request { 
                                src: message.src().unwrap().clone(), 
                                dest: message.dest().unwrap().clone(), 
                                body: MessageType::KeyValueWrite { 
                                    key: target.clone(), 
                                    value: value.clone()
                                } 
                            } ) 
                        } );
                    }
                }
            },
            TransactionTypes::ReadFile => {
                let path = steps.get(&"path".to_string()).unwrap().to_string();
                let file_system_type = steps.get(&"file_type".to_string()).unwrap().to_string();
                let ordinals = steps.get(&"byte_ordinals".to_string()).unwrap().to_string();
                let schema = steps.get(&"schema".to_string()).unwrap().to_string();

                return Ok( Action { 
                    key: TransactionTypes::ReadFile, 
                    value: Some( Message::Request { 
                        src: message.src().unwrap().clone(), 
                        dest: message.dest().unwrap().clone(), 
                        body: MessageType::ReadFromFile { 
                            file_path: path.clone(),
                            accessibility: file_system_type.clone(),
                            bytes: ordinals,
                            schema: schema
                        } 
                    } ) 
                } );
            },
            TransactionTypes::DisplayDf => {
                let df_name = steps.get(&"df_name".to_string()).unwrap().to_string();
                let n_rows = steps.get(&"n_rows".to_string()).unwrap().to_string();

                return Ok( Action { 
                    key: TransactionTypes::DisplayDf, 
                    value: Some( Message::Request { 
                        src: message.src().unwrap().clone(), 
                        dest: message.dest().unwrap().clone(), 
                        body: MessageType::DisplayDf { 
                            df_name,
                            total_rows: n_rows.parse::<usize>().unwrap_or(5),
                        } 
                    } ) 
                } );
            },
            TransactionTypes::GroupBy => {

                // Steps for GroupBy action
                // First compute intermediate results on current node
                // Then send the results to all other nodes to compute final result via fs

                // Read from node datastore
                // Compute aggregation on current node
                // Write intermediate results to node datastore and file system
                // Then read intermediate results from other nodes
                // Recompute by updating intermediate results from node datastore
                // Then return ok()
                let df_name = steps.get(&"df_name".to_string()).unwrap().to_string();
                let aggregation_keys = steps.get(&"keys".to_string()).unwrap().to_string();
                let aggregation_type = steps.get(&"aggregation".to_string()).unwrap().to_string();

                return Ok( Action { 
                    key: TransactionTypes::GroupBy, 
                    value: Some( Message::Request { 
                        src: message.src().unwrap().clone(), 
                        dest: message.dest().unwrap().clone(), 
                        body: MessageType::Aggregate { 
                            df_name,
                            keys: aggregation_keys,
                            agg_type: aggregation_type
                        } 
                    } ) 
                } );
            }
        }
        
    }
}

#[derive(Debug, Clone)]
pub struct Transaction {
    pub id: usize,
    pub state: TransactionState,
    pub actions: Vec<Action>,
    pub locks: Vec<String>, // Keys that this transaction has locked.
}

impl Transaction {

    pub fn new(id: usize) -> Self {
        Transaction {
            id,
            state: TransactionState::Pending,
            actions: Vec::new(),
            locks: Vec::new(),
        }
    }

    // Hardcoding expected parameters passed for each action to have tighter checks when building instructions
    pub fn hash_transaction_steps(&self, action_type: &String, steps: Vec<String>) -> Result<(TransactionTypes, HashMap<String, String>), ClusterExceptions> {
        let mut action_steps: HashMap<String, String> = HashMap::new();

        match action_type.as_str() {
            // First element -- i.e. action -- in list is the type (e.g. "rf")
            x if x == "r" => {

                if steps.len() == 3_usize {
                    action_steps.insert("key".to_string(), steps[1].clone());
                    action_steps.insert("value".to_string(), steps[2].clone());
                    return Ok((TransactionTypes::Read, action_steps));
                } else {
                    return Err(ClusterExceptions::TransactionError(TransactionExceptions::TransactionInstructionSetError));
                }

            },
            x if x == "w" => {      

                if steps.len() == 3_usize {                   
                    action_steps.insert("key".to_string(), steps[1].clone());
                    action_steps.insert("value".to_string(), steps[2].clone());
                    return Ok((TransactionTypes::Write, action_steps));
                } else {
                    return Err(ClusterExceptions::TransactionError(TransactionExceptions::TransactionInstructionSetError));
                }
            },
            x if x == "rf" => {
                if steps.len() == 5_usize {   
                    action_steps.insert("path".to_string(), steps[1].clone());
                    action_steps.insert("file_type".to_string(), steps[2].clone());
                    action_steps.insert("byte_ordinals".to_string(), steps[3].clone());
                    action_steps.insert("schema".to_string(), steps[4].clone());
                    return Ok((TransactionTypes::ReadFile, action_steps));
                } else {
                    return Err(ClusterExceptions::TransactionError(TransactionExceptions::TransactionInstructionSetError));
                }
            },
            x if x == "display-df" => {
                if steps.len() == 3_usize {   
                    action_steps.insert("df_name".to_string(), steps[1].clone());
                    action_steps.insert("n_rows".to_string(), steps[2].clone());
                    return Ok((TransactionTypes::DisplayDf, action_steps));
                } else {
                    return Err(ClusterExceptions::TransactionError(TransactionExceptions::TransactionInstructionSetError));
                }
            },
            x if x == "group-by" => {
                if steps.len() == 4_usize {   
                    action_steps.insert("df_name".to_string(), steps[1].clone());
                    action_steps.insert("keys".to_string(), steps[2].clone());
                    action_steps.insert("aggregation".to_string(), steps[3].clone());
                    return Ok((TransactionTypes::DisplayDf, action_steps));
                } else {
                    return Err(ClusterExceptions::TransactionError(TransactionExceptions::TransactionInstructionSetError));
                }
            },
            x if x == "intermediate-write-to-file" => {
                if steps.len() == 1_usize {   
                    action_steps.insert("file_name".to_string(), steps[1].clone());
                    return Ok((TransactionTypes::DisplayDf, action_steps));
                } else {
                    return Err(ClusterExceptions::TransactionError(TransactionExceptions::TransactionInstructionSetError));
                }
            },
            x if x == "intermediate-write-to-file" => {
                if steps.len() == 1_usize {   
                    action_steps.insert("file_name".to_string(), steps[1].clone());
                    return Ok((TransactionTypes::DisplayDf, action_steps));
                } else {
                    return Err(ClusterExceptions::TransactionError(TransactionExceptions::TransactionInstructionSetError));
                }
            },
            _ => {
                return Err(ClusterExceptions::TransactionError(TransactionExceptions::TransactionInstructionSetError));
            }
        }
    }

    pub fn unhash_transaction_steps(&self, action_type: TransactionTypes, hash_steps: HashMap<String, String>) -> Result<Vec<String>, ClusterExceptions> {
        let mut action_steps: Vec<String> = Vec::new();

        match action_type {
            TransactionTypes::Read => {

                if hash_steps.keys().len() == 3_usize {
                    action_steps.push("r".to_string());
                    action_steps.push(hash_steps.get(&"key".to_string()).unwrap().to_string());
                    action_steps.push(hash_steps.get(&"value".to_string()).unwrap().to_string());
                    return Ok(action_steps);
                } else {
                    return Err(ClusterExceptions::TransactionError(TransactionExceptions::TransactionInstructionSetError));
                }

            },
            TransactionTypes::Write => {      

                if hash_steps.len() == 3_usize {                   
                    action_steps.push("w".to_string());
                    action_steps.push(hash_steps.get(&"key".to_string()).unwrap().to_string());
                    action_steps.push(hash_steps.get(&"value".to_string()).unwrap().to_string());
                    return Ok(action_steps);
                } else {
                    return Err(ClusterExceptions::TransactionError(TransactionExceptions::TransactionInstructionSetError));
                }
            },
            TransactionTypes::ReadFile => {
                if hash_steps.len() == 5_usize {  
                    action_steps.push("rf".to_string());
                    action_steps.push(hash_steps.get(&"path".to_string()).unwrap().to_string());
                    action_steps.push(hash_steps.get(&"file_type".to_string()).unwrap().to_string());
                    action_steps.push(hash_steps.get(&"byte_ordinals".to_string()).unwrap().to_string());
                    action_steps.push(hash_steps.get(&"schema".to_string()).unwrap().to_string());
                    return Ok(action_steps);
                } else {
                    return Err(ClusterExceptions::TransactionError(TransactionExceptions::TransactionInstructionSetError));
                }
            },
            TransactionTypes::DisplayDf => {
                if hash_steps.len() == 3_usize {  
                    action_steps.push("display-df".to_string());
                    action_steps.push(hash_steps.get(&"df_name".to_string()).unwrap().to_string());
                    action_steps.push(hash_steps.get(&"n_rows".to_string()).unwrap().to_string());
                    return Ok(action_steps);
                } else {
                    return Err(ClusterExceptions::TransactionError(TransactionExceptions::TransactionInstructionSetError));
                }
            },
            TransactionTypes::GroupBy => {
                if hash_steps.len() == 4_usize {  
                    action_steps.push("group-by".to_string());
                    action_steps.push(hash_steps.get(&"df_name".to_string()).unwrap().to_string());
                    action_steps.push(hash_steps.get(&"keys".to_string()).unwrap().to_string());
                    action_steps.push(hash_steps.get(&"aggregation".to_string()).unwrap().to_string());
                    return Ok(action_steps);
                } else {
                    return Err(ClusterExceptions::TransactionError(TransactionExceptions::TransactionInstructionSetError));
                }
            }
        }
    }

    pub fn build_instructions(&mut self, transaction_request: Message, other_nodes: Vec<String>) -> Result<(), ClusterExceptions> {

        let instruction_set = transaction_request.body().unwrap().txn();

        if let Some(instructions) = instruction_set {

            for instruction in instructions {

                // Action is first element in instruction set
                let action = &instruction.clone()[0];

                let (action_type, action_steps) = self.hash_transaction_steps(action, instruction.clone())?;

                match action_type {
                    TransactionTypes::Read => {
                        if (action_steps.len() - &instruction[1..].len()) == 0 {

                            if let Ok(built_action) = Action::build(TransactionTypes::Read, &action_steps, transaction_request.clone(), None) {
                                self.actions.push(built_action);
                            } else {
                                return Err(ClusterExceptions::TransactionError(TransactionExceptions::TransactionInstructionSetError));
                            }
                        } else {
                                return Err(ClusterExceptions::TransactionError(TransactionExceptions::TransactionInstructionSetError));
                            }
                    },
                    TransactionTypes::Write => {   
                        if (action_steps.len() - &instruction[1..].len()) == 0 {

                            for other_node in other_nodes.clone().into_iter() {
                                if let Ok(built_action) = Action::build(TransactionTypes::Write, &action_steps, transaction_request.clone(), Some(other_node)) {
                                    self.actions.push(built_action);
                                } else {
                                    return Err(ClusterExceptions::TransactionError(TransactionExceptions::TransactionInstructionSetError));
                                }
                            };

                        } else {
                                return Err(ClusterExceptions::TransactionError(TransactionExceptions::TransactionInstructionSetError));
                            }
                    },
                    TransactionTypes::ReadFile => {                 
                        let byte_ordinals: HashMap<String, (usize, usize)> = serde_json::from_str(&instruction[3])?;

                        // byte_ordinals will contain all nodes so loop here will create a separate request for each
                        for (node, _bytes) in byte_ordinals.clone().into_iter() {
                            let mut updated_message_request = transaction_request.clone();
                            // Update destination node to route transaction actions across network
                            updated_message_request.set_dest(node.clone());
                            if let Ok(built_action) = Action::build(TransactionTypes::ReadFile, &action_steps, updated_message_request, Some(node)) {
                                self.actions.push(built_action);
                            } else {
                                return Err(ClusterExceptions::TransactionError(TransactionExceptions::TransactionInstructionSetError));
                            }
                        };
                    },
                    TransactionTypes::DisplayDf => {
                        let mut all_nodes: Vec<String> = vec![];
                        if let Some(first_node) = transaction_request.dest() {
                            all_nodes.push(first_node.clone());

                            // Only extend all_nodes with elements from other_nodes if other_nodes contains additional node_ids than first_node
                            // get_nodes() is the function call that produces param other_nodes so if "n1" is the only node, it will appear
                            // first in transaction_request.dest() and then in other_nodes as we do not filter the return list when calling other_nodes
                            if other_nodes.iter().filter(|x| *x != first_node).map(|x| x.to_string()).collect::<Vec<String>>().len() > 0 {
                                all_nodes.extend(other_nodes.clone());
                            }
                            
                        } else {
                            return Err(ClusterExceptions::TransactionError(TransactionExceptions::TransactionInstructionSetError));
                        }
                        for node in all_nodes.clone().into_iter() {
                            let mut updated_message_request = transaction_request.clone();

                            // Update destination node to route transaction actions across network
                            updated_message_request.set_dest(node.clone());
                            if let Ok(built_action) = Action::build(TransactionTypes::DisplayDf, &action_steps, updated_message_request, None) {
                                self.actions.push(built_action);
                            } else {
                                return Err(ClusterExceptions::TransactionError(TransactionExceptions::TransactionInstructionSetError));
                            }
                        };
                    },
                     TransactionTypes::GroupBy => {
                        let mut all_nodes: Vec<String> = vec![];
                        if let Some(first_node) = transaction_request.dest() {
                            all_nodes.push(first_node.clone());

                            // Only extend all_nodes with elements from other_nodes if other_nodes contains additional node_ids than first_node
                            // get_nodes() is the function call that produces param other_nodes so if "n1" is the only node, it will appear
                            // first in transaction_request.dest() and then in other_nodes as we do not filter the return list when calling other_nodes
                            if other_nodes.iter().filter(|x| *x != first_node).map(|x| x.to_string()).collect::<Vec<String>>().len() > 0 {
                                all_nodes.extend(other_nodes.clone());
                            }
                            
                        } else {
                            return Err(ClusterExceptions::TransactionError(TransactionExceptions::TransactionInstructionSetError));
                        }
                        for node in all_nodes.clone().into_iter() {
                            let mut updated_message_request = transaction_request.clone();

                            // Update destination node to route transaction actions across network
                            updated_message_request.set_dest(node.clone());
                            if let Ok(built_action) = Action::build(TransactionTypes::GroupBy, &action_steps, updated_message_request, None) {
                                self.actions.push(built_action);
                            } else {
                                return Err(ClusterExceptions::TransactionError(TransactionExceptions::TransactionInstructionSetError));
                            }
                        }
                    }
                }

            }

        Ok(())

    } else {
        return Err(ClusterExceptions::TransactionError(TransactionExceptions::TransactionInstructionSetError));
    }

}

}

#[derive(Debug, Clone)]
pub struct LockManager {
    pub locks: HashMap<String, usize>, // Key -> Transaction ID.
}

impl LockManager {
    pub fn new() -> Self {
        LockManager {
            locks: HashMap::new(),
        }
    }

    pub fn acquire_lock(&mut self, node: &str, transaction_id: usize) -> Result<(), ClusterExceptions> {
        if let Some(owner) = self.locks.get(node) {
            if *owner != transaction_id {
                return Err(ClusterExceptions::TransactionError(TransactionExceptions::TransactionLockAlreadyAcquired));
            }
        }
        self.locks.insert(node.to_string(), transaction_id);
        Ok(())
    }

    pub fn release_lock(&mut self, node: &str, transaction_id: usize) {
        if let Some(owner) = self.locks.get(node) {
            if *owner == transaction_id {
                self.locks.remove(node);
            }
        }
    }
}

#[derive(Debug)]
pub struct TransactionManager {
    pub file_system_manager: FileSystemManager,
    pub query_plan: QueryPlan,
    pub active_transactions: HashMap<usize, Transaction>,
    pub lock_manager: LockManager,
    pub wal: Arc<Mutex<WriteAheadLog>>,
    pub database: HashMap<TransactionTypes, Message>,
}

impl TransactionManager {
    pub fn new(wal_path: &str, accessibility_type: &FileSystemType, local_working_directory: &String) -> Self {
        TransactionManager {
            file_system_manager: FileSystemManager::new(accessibility_type, local_working_directory.clone()),
            query_plan: QueryPlan::new(),
            active_transactions: HashMap::new(),
            lock_manager: LockManager::new(),
            wal: Arc::new(Mutex::new(WriteAheadLog::new(wal_path).expect("Failed to initialize WAL"))),
            database: HashMap::new(),
        }

    }

    // pub async fn process_plan() {
    //     // Process each step in the query plan

    //     for (step_idx, step_details) in self.query_plan_steps.clone().into_iter() {
    //         for (sub_step_idx, (node_id, plan_type, message_string, status)) in step_details.into_iter() {

    //             match plan_type {
    //                 QueryPlanTypes::Aggregate => {
    //                     // Send aggregate message to node
    //                 },
    //                 QueryPlanTypes::WriteToFile => {
    //                     // Send write to file message to node
    //                 },
    //                 QueryPlanTypes::ReadFromFile => {
    //                     // Send read from file message to node
    //                 },
    //                 QueryPlanTypes::AggregateExtend => {
    //                     // Send aggregate extend message to node
    //                 }
    //             }

    //         }
    //     }
    // }

pub async fn start_transaction(&mut self, transaction_request: Message, node_topology: Vec<String>) -> Result<usize, ClusterExceptions> {
        let id = self.active_transactions.len() as usize + 1;

        // Receive incoming request -- which can be actual message in JSON or command that maps to several messages -- as CLIType
        // Incoming request then wrapped in transaction
        // This will include transaction id, instructions, req/response handling, and commit/abort logic


        let transaction = Transaction::new(id);
        self.active_transactions.insert(id, transaction);

        if let Some(new_transaction) = self.active_transactions.get_mut(&id) {
            new_transaction.build_instructions(transaction_request, node_topology)?;
        };

        // Write to WAL
        if let Err(_error) = self.wal
            .lock()
            .await
            .write_entry(&WalEntry::TransactionStart { transaction_id: id }) {
                return Err(ClusterExceptions::TransactionError(TransactionExceptions::FailedToWriteToWal { error_message: id.to_string() }));
        };
        Ok(id)
    }

    pub async fn execute_transaction(&mut self, node_id: String, transaction_id: usize) -> Result<Vec<Action>, ClusterExceptions> {

        if let Some(transaction) = self.active_transactions.get_mut(&transaction_id) {
            self.lock_manager.acquire_lock(node_id.as_str(), transaction_id)?;

            transaction.locks.push(node_id.to_string());

            
            for action in transaction.actions.clone().into_iter() {
                
                // Log the actions in WAL
                if let Err(error) = self.wal
                    .lock()
                    .await
                    .write_entry(&WalEntry::ActionLog {
                    transaction_id,
                    node: action.value.clone().unwrap().src().unwrap().clone(),
                    action: action.clone(),
                }) {
                    return Err(ClusterExceptions::TransactionError(TransactionExceptions::FailedToWriteToWal { error_message: transaction_id.to_string() }));
                };


            }
            Ok(transaction.actions.to_vec())
        } else {
            return Err(ClusterExceptions::TransactionError(TransactionExceptions::TransactionNotFound));
        }
    }

    pub async fn commit_transaction(&mut self, transaction_id: usize) -> Result<(), ClusterExceptions> {
        if let Some(transaction) = self.active_transactions.get_mut(&transaction_id) {

            // Apply actions to the database
            for action in &transaction.actions {
                if let Some(value) = &action.value {
                    self.database.insert(action.key.clone(), value.clone());
                } else {
                    self.database.remove(&action.key);
                }
            }

            // Update WAL
            self.wal
                .lock()
                .await
                .write_entry(&WalEntry::TransactionCommit { transaction_id })
                .expect("Failed to write to WAL");

            // Release locks and remove transaction
            for key in &transaction.locks {
                self.lock_manager.release_lock(key, transaction_id);
            }
            self.active_transactions.remove(&transaction_id);
            Ok(())
        } else {
            return Err(ClusterExceptions::TransactionError(TransactionExceptions::TransactionNotFound));
        }
    }

    pub async fn abort_transaction(&mut self, transaction_id: usize) {
        if let Some(transaction) = self.active_transactions.get_mut(&transaction_id) {
            // Update WAL
            self.wal
                .lock()
                .await
                .write_entry(&WalEntry::TransactionAbort { transaction_id })
                .expect("Failed to write to WAL");

            // Release locks and remove transaction
            for key in &transaction.locks {
                self.lock_manager.release_lock(key, transaction_id);
            }
            self.active_transactions.remove(&transaction_id);
        }
    }

    

}


#[derive(Debug, Clone)]
pub enum TransactionExceptions {
    TransactionInstructionSetError,
    TransactionLockAlreadyAcquired,
    TransactionAlreadyFinalized,
    TransactionNotFound,
    FailedToWriteToWal { error_message: String },
    FailedToUpdateWal { error_message: String },
    FailedToCommitTransaction { error_message: String },
    QueryPlanBuildError { error_message: String },
    TransactionManagerNotInitialized { error_message: String },
}


impl fmt::Display for TransactionExceptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransactionExceptions::TransactionInstructionSetError => write!(f, "3 values per transaction instruction are expected."),
            TransactionExceptions::TransactionLockAlreadyAcquired => write!(f, "Transaction lock already acquired."),
            TransactionExceptions::TransactionAlreadyFinalized => write!(f, "Transaction has already been finalized."),
            TransactionExceptions::TransactionNotFound => write!(f, "Attempted transaction cannot be found."),
            TransactionExceptions::FailedToWriteToWal { error_message} => write!(f, "Failed to write transaction '{}' to WAL.", error_message),
            TransactionExceptions::FailedToUpdateWal { error_message} => write!(f, "Failed to update WAL with transaction '{}'.", error_message),
            TransactionExceptions::FailedToCommitTransaction { error_message} => write!(f, "Failed to commit transaction '{}'.", error_message),
            TransactionExceptions::QueryPlanBuildError { error_message} => write!(f, "Failed to build query plan for command '{}'.", error_message),
            TransactionExceptions::TransactionManagerNotInitialized { error_message} => write!(f, "Transaction manager not initialized: '{}'.", error_message),
        }
    }
}
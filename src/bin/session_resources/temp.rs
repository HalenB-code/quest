use std::fmt;
use std::collections::{HashMap, BTreeMap};
use serde::{Serialize, Deserialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use serde_json;

use crate::session_resources::message::{MessageFields, MessageType, MessageTypeFields, Message, MessageStatus};
use crate::session_resources::exceptions::ClusterExceptions;
use crate::session_resources::write_ahead_log::{WalEntry, WriteAheadLog};
use crate::session_resources::cli::ClusterCommand;
use crate::session_resources::cluster::Cluster;
use crate::session_resources::file_system::{FileSystemManager};

// const WAL_PATH: &str = r"C:\rust\projects\rust-bdc";
const OVERWRITE_INCOMING_MSG_ID: bool = true;

#[derive(Debug, Clone)]
pub struct QueryPlan<'a> {
    cluster_reference: &'a Cluster,
    query_plan_id: usize,
    query_plan_steps: BTreeMap<usize, HashMap<usize, (String, QueryPlanTypes, String, QueryPlanStatus)>>, // usize is step of query plan; usize is sub-step of query plan step; String = node_id, QueryPlanTypes, String = message string, QueryPlanType Status
    query_plan_status: QueryPlanStatus,
}

#[derive(Debug, Clone)]
pub enum QueryPlanTypes {
    Aggregate,
    WriteToFile,
    ReadFromFile,
    AggregateExtend,
    DisplayDf
}

#[derive(Debug, Clone)]
pub enum QueryPlanStatus {
    Pending,
    Complete,
    Failed
}

impl<'a> QueryPlan<'a> {

    pub fn new(cluster_reference: &'a Cluster) -> Self {

        QueryPlan {
            cluster_reference,
            query_plan_id: 0,
            query_plan_steps: BTreeMap::new(),
            query_plan_status: QueryPlanStatus::Pending,
        }

    }

    pub async fn plan(&mut self, cluster_command: ClusterCommand) -> Result<(), ClusterExceptions> {

        match cluster_command {

            ClusterCommand::CmdGroupBy { target_name, aggregation_keys, aggregation_type } => {

                let query_plan_idx = self.query_plan_steps.len() + 1;
                let mut query_plan_steps = HashMap::new();

                // Step 1: Aggregate
                let query_plan_step_idx = query_plan_steps.len() + 1;
                // Assuming here target_name is already in node datastore
                if let Some(mut message_request_string) = Message::default_request_message("Aggregate") {
                    message_request_string.set_body(MessageType::Aggregate {
                        df_name: target_name.clone(),
                        keys: aggregation_keys.clone(),
                        agg_type: aggregation_type.clone(),
                    });
                    query_plan_steps.insert(query_plan_step_idx, (target_name.clone(), QueryPlanTypes::Aggregate, message_request_string, QueryPlanStatus::Pending));

                } else {
                    return Err(ClusterExceptions::InvalidCommand { error_message: "aggregate".to_string() });
                }

                // Step 2: Write to file
                if let Some(mut message_request_string) = Message::default_request_message("WriteToFile") {
                    // Need a method to lookup at the cluster level the datastore target to retrieve the schema
                    // Otherwise a separate messaged must be sent to a node to retrieve the schema and that costs more than storing metadata in the cluster

                    message_request_string.set_body(MessageType::WriteToFile {
                        file_path: format!("{}/{}/intermediate_data_file.csv", self.cluster_reference.cluster_configuration.working_directory.local_path.clone(), target_name),
                        file_format: aggregation_type.clone(),
                        // Schema
                    });
                    let query_plan_step_idx = query_plan_steps.len() + 1;

                    query_plan_steps.insert(query_plan_step_idx, (target_name.clone(), QueryPlanTypes::WriteToFile, message_request_string, QueryPlanStatus::Pending));

                } else {
                    return Err(ClusterExceptions::InvalidCommand { error_message: "write_to_file".to_string() });
                }

                // Step 3: Read from file
                if let Some(mut message_request_string) = Message::default_request_message("ReadFromFile") {
                    // Assuming the file is stored in the local file system
                    message_request_string.set_body(MessageType::ReadFromFile {
                        file_path: format!("{}/{}/intermediate_data_file.csv", self.cluster_reference.cluster_configuration.working_directory.local_path.clone(), target_name),
                        accessibility: "local".to_string(), // Assuming local for now
                        bytes: "".to_string(), // Placeholder for byte ordinals
                        schema: "".to_string(), // Placeholder for schema
                    });

                    let query_plan_step_idx = query_plan_steps.len() + 1;
                    query_plan_steps.insert(query_plan_step_idx, (target_name.clone(), QueryPlanTypes::ReadFromFile, message_request_string, QueryPlanStatus::Pending));

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
                    query_plan_steps.insert(query_plan_step_idx, (target_name.clone(), QueryPlanTypes::AggregateExtend, message_request_string, QueryPlanStatus::Pending));

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
                    query_plan_steps.insert(query_plan_step_idx, (target_name.clone(), QueryPlanTypes::AggregateExtend, message_request_string, QueryPlanStatus::Pending));

                } else {
                    return Err(ClusterExceptions::InvalidCommand { error_message: "aggregate-extend".to_string() });
                }

                Ok(())

            },
            ClusterCommand::CmdDisplayDf { target_name, target_node, n_rows } => {
                let query_plan_idx = self.query_plan_steps.len() + 1;
                let mut query_plan_steps = HashMap::new();

                // Step 1: Display DataFrame
                let query_plan_step_idx = query_plan_steps.len() + 1;

                if target_node != "all" {
                    if let Some(mut message_request_string) = Message::default_request_message("DisplayDf") {
                        message_request_string.set_dest(target_node.to_string());
                        message_request_string.set_body(MessageType::DisplayDf {
                            df_name: target_name.clone(),
                            total_rows: n_rows,
                        });

                        query_plan_steps.insert(query_plan_step_idx, (target_name.clone(), QueryPlanTypes::DisplayDf, message_request_string, QueryPlanStatus::Pending));
                    } else {
                        return Err(ClusterExceptions::InvalidCommand { error_message: "display-df".to_string() });
                    }
                    
                } else {

                    // If target_node is "all", then we need to send the request to all nodes
                    let all_nodes = self.cluster_reference.get_nodes();

                    for node in all_nodes.iter() {
                        if let Some(mut message_request_string) = Message::default_request_message("DisplayDf") {
                            message_request_string.set_dest(node.clone());
                            message_request_string.set_body(MessageType::DisplayDf {
                                df_name: target_name.clone(),
                                total_rows: n_rows,
                            });
                        query_plan_steps.insert(query_plan_step_idx, (node.clone(), QueryPlanTypes::DisplayDf, message_request_string, QueryPlanStatus::Pending));
                        }
                    }
                }

                Ok(())
            }, 
            ClusterCommand::CmdReadFile { target_file_path, target_node, delimiter } => {
                let query_plan_idx = self.query_plan_steps.len() + 1;
                let mut query_plan_steps = HashMap::new();

                let separator: u8;

                // TODO: Support other delimiters
                separator = ",".to_string().as_bytes()[0];

                let file_system_type = self.cluster_reference.cluster_configuration.working_directory.file_system_type.clone();
                let infered_file_schema = FileSystemManager::get_file_header(self.cluster_reference.cluster_configuration.working_directory.local_path.clone(), separator)?;
                let infered_file_schema_string = serde_json::to_string(&infered_file_schema)?;


                // Step 1: Read from file
                let mut query_plan_step_idx = query_plan_steps.len() + 1;

                if target_node != "all" {

                    let nodes = target_node.split(',').collect::<Vec<&str>>();

                    let file_path_hash = self.cluster_reference.file_system_manager.read_from_file(self.cluster_reference.cluster_configuration.working_directory.local_path.clone(), &self.cluster_reference.get_nodes())?;
                    let byte_ordinals = FileSystemManager::get_byte_ordinals(self.cluster_reference.cluster_configuration.working_directory.local_path.clone(), &self.cluster_reference.get_nodes())?;
                    let byte_ordinals_string = serde_json::to_string(&byte_ordinals)?;

                    for node in nodes.iter() {
                        let target_node = node.to_string();
                        if let Some(mut message_request_string) = Message::default_request_message("ReadFromFile") {
                            message_request_string.set_dest(target_node.clone());
                            message_request_string.set_body(MessageType::ReadFromFile {
                                file_path: target_file_path.clone(),
                                accessibility: "local".to_string(), // Assuming local for now
                                bytes: byte_ordinals_string,
                                schema: infered_file_schema_string,
                            });
                            query_plan_steps.insert(query_plan_step_idx, (target_file_path.clone(), QueryPlanTypes::ReadFromFile, message_request_string, QueryPlanStatus::Pending));

                            query_plan_step_idx += 1;
                        }
                    }

                } else {

                    // If target_node is "all", then we need to send the request to all nodes
                    let all_nodes = self.cluster_reference.get_nodes();

                    let file_path_hash = self.cluster_reference.file_system_manager.read_from_file(self.cluster_reference.cluster_configuration.working_directory.local_path.clone(), &self.cluster_reference.get_nodes())?;
                    let byte_ordinals = FileSystemManager::get_byte_ordinals(self.cluster_reference.cluster_configuration.working_directory.local_path.clone(), &self.cluster_reference.get_nodes())?;
                    let byte_ordinals_string = serde_json::to_string(&byte_ordinals)?;

                    for node in all_nodes.iter() {
                        if let Some(mut message_request_string) = Message::default_request_message("ReadFromFile") {
                            message_request_string.set_dest(node.clone());
                            message_request_string.set_body(MessageType::ReadFromFile {
                                file_path: target_file_path.clone(),
                                accessibility: "local".to_string(), // Assuming local for now
                                bytes: byte_ordinals_string,
                                schema: infered_file_schema_string,
                            });
                            query_plan_steps.insert(query_plan_step_idx, (node.clone(), QueryPlanTypes::ReadFromFile, message_request_string, QueryPlanStatus::Pending));

                            query_plan_step_idx += 1;
                        }
                    }

                }

                Ok(())
            },
            ClusterCommand::CmdLogCluster { logging_target } => {
                Ok(())
            },
            ClusterCommand::CmdLogNode { logging_target } => {
                Ok(())
            },
            ClusterCommand::CmdMessageString { message } => {

                let query_plan_idx = self.query_plan_steps.len() + 1;
                let mut query_plan_steps = HashMap::new();

                // Step 1: Aggregate
                let query_plan_step_idx = query_plan_steps.len() + 1;

                Ok(())

            },
            _ => {
                Err(ClusterExceptions::InvalidCommand { error_message: "display-df".to_string() })
            }

        }

    }

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


    pub async fn process_plan() {
        // Process each step in the query plan

        for (step_idx, step_details) in self.query_plan_steps.clone().into_iter() {
            for (sub_step_idx, (node_id, plan_type, message_string, status)) in step_details.into_iter() {

                match plan_type {
                    QueryPlanTypes::Aggregate => {
                        // Send aggregate message to node
                    },
                    QueryPlanTypes::WriteToFile => {
                        // Send write to file message to node
                    },
                    QueryPlanTypes::ReadFromFile => {
                        // Send read from file message to node
                    },
                    QueryPlanTypes::AggregateExtend => {
                        // Send aggregate extend message to node
                    }
                }

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
pub struct Transaction<'a> {
    pub id: usize,
    pub state: TransactionState,
    pub actions: QueryPlan<'a>,
    pub locks: Vec<String>, // Keys that this transaction has locked.
}

impl<'a>  Transaction<'a> {

    pub fn new(id: usize, cluster_reference: &'a Cluster) -> Self {
        Transaction {
            id,
            state: TransactionState::Pending,
            actions: QueryPlan::new(cluster_reference),
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

                        let query_plan = QueryPlaner::plan("group-by").into();
                        
                        // For each action in query plan, create a transaction action
                        for (step, value) in query_plan.clone().into_iter() {

                            // For each node in all_nodes, create a transaction action
                            for (idx, node) in all_nodes.clone().into_iter().enumerate() {

                                match step {
                                    "Aggregate" => {

                                        // Update destination node to route transaction actions across network
                                        updated_message_request.set_dest(node.clone());

                                        if let Ok(built_action) = Action::build(TransactionTypes::GroupBy, &action_steps, updated_message_request, None) {
                                            self.actions.push(built_action);
                                        } else {
                                            return Err(ClusterExceptions::TransactionError(TransactionExceptions::TransactionInstructionSetError));
                                        }
                                    },
                                    "WriteToFile" => {
                                        // TODO: schema must accompany file path as input param to intermediate-write-to-file; this is so the same schema can be used to read the file later
                                        let (action_type, action_steps_additional) = self.hash_transaction_steps(&"intermediate-write-to-file".to_string(), format!("{}_ABC123EFG.csv", node))?;

                                        if let Ok(built_action) = Action::build(TransactionTypes::GroupBy, &action_steps_additional, updated_message_request, None) {
                                            self.actions.push(built_action);
                                        } else {
                                            return Err(ClusterExceptions::TransactionError(TransactionExceptions::TransactionInstructionSetError));
                                        }

                                    },
                                    "ReadFromFile" => {
                                        // Read from file system
                                        let (action_type, action_steps_additional) = self.hash_transaction_steps(&"intermediate-write-to-file".to_string(), format!("{}_ABC123EFG.csv", node))?;
                                    },
                                    "AggregateExtend" => {
                                        // Extend aggregation with intermediate results
                                        action_steps.insert(step.to_string(), value.to_string());
                                    },
                                    _ => {}
                                }
                                action_steps.insert(step, value.to_string());
                            }
                            let mut updated_message_request = transaction_request.clone();


                        };
                    }
                }
            }

        }

        Ok(())

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

#[derive(Debug, Clone)]
pub struct TransactionManager<'a> {
    pub cluster_reference: &'a Cluster,
    pub active_transactions: HashMap<usize, Transaction<'a>>,
    pub lock_manager: LockManager,
    pub node_message_log: HashMap<usize, (Message, String)>
    pub wal: Arc<Mutex<WriteAheadLog>>, 
    pub database: HashMap<usize, QueryPlan<'a>>,
}

impl<'a> TransactionManager<'a> {
    pub fn new(wal_path: &str, cluster_reference: &'a Cluster) -> Self {
        TransactionManager {
            cluster_reference: cluster_reference,
            active_transactions: HashMap::new(),
            lock_manager: LockManager::new(),
            node_message_log: HashMap::new(),
            wal: Arc::new(Mutex::new(WriteAheadLog::new(wal_path).expect("Failed to initialize WAL"))),
            database: HashMap::new(),
        }
    }

    pub async fn insert_message_log(&mut self, message: Message) -> Result<usize, ClusterExceptions> {

        let message_id = self.node_message_log.keys().max().unwrap_or(&0_usize) + 1;

        // Insert message in log as pending
        self.node_message_log.insert(message_id, (message.clone(), MessageStatus::Pending.to_string()));

        Ok(message_id)
    }

    pub async fn update_message_log(&mut self, message_id: usize, message_status: MessageStatus) -> Result<(), ClusterExceptions> {

        match message_status {
            MessageStatus::Ok => {
            self.node_message_log
            .entry(message_id)
            .and_modify(|(_key, value)| {
                *value = MessageStatus::Ok.to_string();
            });
            },
            MessageStatus::Failed => {
            self.node_message_log
            .entry(message_id)
            .and_modify(|(_key, value)| {
                *value = MessageStatus::Failed.to_string();
            });
            }
            _ => {
            return Err(ClusterExceptions::MessageStatusNotUpdated {
                error_message: message_id.to_string(),
            })
            }
        }

        Ok(())
    }


    pub async fn categorize_and_queue(&mut self, incoming_message: String) -> Result<(), ClusterExceptions> {

        let mapped_message = ClusterCommand::map_command_line_request(incoming_message)?;

        let mut message_request = QueryPlan::new(self.cluster_reference);
        let message_id = self.insert_message_log(message_request.clone()).await?;

        if OVERWRITE_INCOMING_MSG_ID {
            message_request.set_msg_id(message_id);
        }

        match message_request.dest() {
            None => {
            println!("{:?}", message_request.msg_type().unwrap());
            match message_request.msg_type().unwrap().as_str() {
                "log_cluster_messages" => {
                
                if let Ok(()) = self.log_messages() {
                    let message_response = Message::Response {
                    src: message_request.dest().unwrap().to_string(),
                    dest: message_request.src().unwrap().to_string(),
                    body: MessageType::LogClusterMessagesOk {
                    }
                };
                    self.cluster_response(message_response)?;
                }            
                },
                "init" => {
                self.process_local(message_id, message_request).await?;
                }
                _ => {
                }
            }
            },
            Some(_destination) => {
            if let Some((_node, node_type)) = self.network_manager.network_map.get(message_request.dest().unwrap()) {
                match node_type {
                    NodeType::Local => {
                    self.process_local(message_id, message_request).await?;
                    },
                    NodeType::Remote => {
                    self.process_remote(message_id, message_request).await?;
                    }
                }
            } else {
                // If request is init or another type, we don't need confirmation node exists
                self.update_message_log(message_id, MessageStatus::Failed).await?;
                return Err(ClusterExceptions::NodeDoesNotExist { error_message: message_request.dest().unwrap().clone() });
            }
            }
        }

        Ok(())
    }

    pub async fn process_local(&mut self, message_id: usize, message_request: Message) -> Result<(), ClusterExceptions> {

    match message_request {

        Message::Init { .. } => {
            if let Err(error) = self.process_initialization(message_request).await {
            self.update_message_log(message_id, MessageStatus::Failed).await?;
            return Err(error);
            } else {
            self.update_message_log(message_id, MessageStatus::Ok).await?;
            return Ok(());
            };
        },
        Message::Request { .. } => {
        match message_request.msg_type().unwrap().as_str() {
            "txn" => {
            if let Err(error) = self.categorize_and_queue_transactions(message_request).await {
                self.update_message_log(message_id, MessageStatus::Failed).await?;
                return Err(error);
            } else {
                self.update_message_log(message_id, MessageStatus::Ok).await?;
                return Ok(());
            };
            },
            _ => {
            if let Err(error) = self.messenger.request_queue(message_request).await {
                self.update_message_log(message_id, MessageStatus::Failed).await?;
                return Err(error);
            } else {
                self.update_message_log(message_id, MessageStatus::Ok).await?;
                return Ok(());
            };
            }

        }
        },
        Message::Response { .. } => {
        return Err(ClusterExceptions::UnkownClientRequest {
            error_message: message_request.dest().unwrap().to_string(),
        });
        }
    }
    }

    pub async fn poll_remote_responses(&mut self, remote_sender_id: &String) -> Result<Vec<Message>, ClusterExceptions> {
        println!("In poll_remote");
        let mut buffer: Vec<Message> = Vec::with_capacity(100);
        let limit = 100;

        match self.network_manager.network_response_readers.get(remote_sender_id.as_str()) {
            Some(node_receiver) => {
                println!("Now polling for response...");
                let mut node_receiver_lock = node_receiver.lock().await;

                // Wait for a response with a timeout
                match timeout(Duration::from_secs(10), node_receiver_lock.recv()).await {
                    Ok(messages) => {
                        if let Some(vector_messages) = messages {
                        println!("Remote TCP response: {:?}", vector_messages);
                        return Ok(vector_messages);
                        } else {
                        return Err(ClusterExceptions::RemoteNodeRequestError { error_message: format!("Channel closed for node {}", remote_sender_id) });
                        }
                    },
                    Err(_) => {
                        return Err(ClusterExceptions::RemoteNodeRequestError {
                            error_message: format!(
                                "Timeout while waiting for response from node {}",
                                remote_sender_id
                            ),
                        });
                    }
                }
            }
            None => {
                println!("No connection to poll...");
                return Err(ClusterExceptions::RemoteNodeRequestError {
                    error_message: format!("Connection not found for node {}", remote_sender_id),
                });
            }
        }
    }

    pub async fn process_remote(&mut self, message_id: usize, message_request: Message) -> Result<(), ClusterExceptions> {

    let message_request_clone = message_request.clone();
    let message_type = message_request_clone.msg_type().unwrap();

    // TODO: Add TX loop to handle transaction type
    // Loop should call complete transaction fn below
    match message_type.as_str() {
        "txn" => {
        self.complete_transaction_remote(message_id, message_request).await?;
        return Ok(());
        },
        _ => {
        
        let node_id = message_request_clone.dest().unwrap();

        if let Some(remote_sending_channel) = &self.network_manager.network_request_writers.get(node_id) {

                match remote_sending_channel.send(message_request.clone()).await {
                Ok(()) => {
                println!("Message sent to {}", node_id);
                match self.poll_remote_responses(node_id).await {
                    Ok(message_responses) => {
                    println!("poll remote returned");

                    for message_response in message_responses {
                        match message_response.body().unwrap().is_ok() {
                        true => {
                            self.update_message_log(message_id, MessageStatus::Ok).await?;
                        },
                        false => {
                            // If one of the responses from the remote node is a follow-up request, add this to the response queue
                            //self.messenger.response_queue(message_response).await?;
                            self.update_message_log(message_id, MessageStatus::Failed).await?;
                        }
                        };
                    }
                    return Ok(());
                    },
                    Err(error) => {
                    println!("Send failed");
                    return Err(ClusterExceptions::RemoteNodeRequestError { error_message: error.to_string() });
                    }
                };
                },
                Err(error) => {
                println!("Remote request sent and err");
                return Err(ClusterExceptions::RemoteNodeRequestError { error_message: error.to_string() });
                }
            } 
            } else {
            println!("No sending channel found for node {}", node_id);
                return Err(ClusterExceptions::RemoteNodeRequestError { error_message: "no sending channel".to_string() });
            }
        }
    };

    }


    pub async fn complete_transaction_remote(&mut self, message_id: usize, message_request: Message) -> Result<(), ClusterExceptions> {

        let node_topology = self.get_nodes();
        let transaction_id = self.transaction_manager.start_transaction(message_request.clone(), node_topology).await?;
        let transaction_actions = self.transaction_manager.execute_transaction(message_request.dest().unwrap().clone(), transaction_id).await?;
        let mut transaction_execution_error = None;

        println!("Transaction actions: {:?}", transaction_actions);

        for action in transaction_actions {

        if let Some(transaction_message_request) = action.value {

            let node_id = transaction_message_request.dest().unwrap();

            if let Some(remote_sending_channel) = &self.network_manager.network_request_writers.get(node_id) {

            match remote_sending_channel.send(transaction_message_request.clone()).await {
                Ok(()) => {

                match self.poll_remote_responses(node_id).await {
                    Ok(message_responses) => {

                    for message_response in message_responses {
                        match message_response.body().unwrap().is_ok() {
                        true => {
                            ()
                        },
                        false => {
                            // If one of the responses from the remote node is a follow-up request, add this to the response queue
                            transaction_execution_error = Some(());
                        }
                        };
                    }
                    },
                    Err(_error) => {
                    transaction_execution_error = Some(());;
                    }
                };
                },
                Err(_error) => {
                transaction_execution_error = Some(());;
                }
            } 
        } else {
            transaction_execution_error = Some(());;
        }

        }

    };

    if let None = transaction_execution_error {
        self.transaction_manager.commit_transaction(transaction_id).await?;
        self.update_message_log(message_id, MessageStatus::Ok).await?;
        return Ok(())
    } else {
        self.update_message_log(message_id, MessageStatus::Failed).await?;
        return Err(ClusterExceptions::TransactionError(TransactionExceptions::FailedToCommitTransaction { error_message: transaction_id.to_string() }));
    }

    }

    pub async fn complete_transaction(&mut self, message_request: Message) -> Result<(), ClusterExceptions> {

        let node_topology = self.get_nodes();

        let transaction_id = self.transaction_manager.start_transaction(message_request.clone(), node_topology).await?;
        let transaction_actions = self.transaction_manager.execute_transaction(message_request.dest().unwrap().clone(), transaction_id).await?;

        let mut transaction_execution_error = None;

        for action in transaction_actions {

            if let Some(transaction_message_request) = action.value {
            if let Err(_error) = self.messenger.request_queue(transaction_message_request).await {
                transaction_execution_error = Some(())
            }
            }

        };

        if let None = transaction_execution_error {
            self.transaction_manager.commit_transaction(transaction_id).await?;
            Ok(())
        } else {
            return Err(ClusterExceptions::TransactionError(TransactionExceptions::FailedToCommitTransaction { error_message: transaction_id.to_string() }));
        }

    }

    pub async fn categorize_and_queue_transactions(&mut self, txn_message: Message) -> Result<(), ClusterExceptions> {

        // TODO Assign msg_id based on global log and set status to out

        self.complete_transaction(txn_message).await?;

        // TODO If no error comes back, set status to ok in global log

        Ok(())
    }

    pub async fn propagate_message(&mut self, message: Message) -> Result<(), ClusterExceptions> {

        if let Some(requeue_node_id) = message.dest() {

            match &self.nodes.contains_key(requeue_node_id) {
                true => {
                    self.messenger.request_queue(message.clone()).await?;
                },
                false => {
                    let init_message = self.messenger.categorize(format!(r#"{{"type":"init","msg_id":1,"node_id":"{requeue_node_id}","node_ids":[]}}"#)).await?;
                    self.messenger.response_queue(init_message).await?;
                    self.messenger.response_queue(message.clone()).await?;
                    // println!("In propagate {:?}", &self.messenger.message_responses.lock().await);
                    ()
                }
                
                }

            } else {
                // If the node does not exist, return an error
                return Err(ClusterExceptions::NodeDoesNotExist {
                    error_message: format!("{:?}", message),
                });
            }

        Ok(())
    }

    pub async fn process_followups(&mut self) -> Result<(), ClusterExceptions> {

        let cluster_messenger = self.messenger.clone();
        let mut messenger_request_queue_lock = cluster_messenger.message_responses.lock().await;
        while let Some(response_request) = messenger_request_queue_lock.pop_front() {
            match response_request {
                Message::Init { .. } => {
                    self.process_initialization(response_request).await?;
                },
                Message::Request { .. } => {
                self.messenger.request_queue(response_request).await?;
                },
                Message::Response { .. } => {
                return Err(ClusterExceptions::UnkownClientRequest {
                    error_message: response_request.dest().unwrap().to_string(),
                });
                }
            }
        }

        Ok(())

        }
        

    pub async fn recover_from_wal(transaction_manager: &mut TransactionManager<'_>, wal_path: &str) {
    let entries = WriteAheadLog::replay(wal_path).expect("Failed to replay WAL");
    for entry in entries {
        match entry {
            WalEntry::TransactionStart { transaction_id } => {
                transaction_manager.active_transactions.insert(transaction_id, Transaction::new(transaction_id));
            }
            WalEntry::ActionLog { transaction_id, action, .. } => {
                if let Some(transaction) = transaction_manager.active_transactions.get_mut(&transaction_id) {
                    transaction.actions.push(action);
                }
            }
            WalEntry::TransactionCommit { transaction_id } => {
                transaction_manager.commit_transaction(transaction_id).await.ok();
            }
            WalEntry::TransactionAbort { transaction_id } => {
                transaction_manager.abort_transaction(transaction_id).await;
            }
        }
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
        }
    }
}
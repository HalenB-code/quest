use std::fmt;
use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use serde_json;

use crate::session_resources::message::{MessageFields, MessageType, MessageTypeFields, Message};
use crate::session_resources::exceptions::ClusterExceptions;
use crate::session_resources::write_ahead_log::{WalEntry, WriteAheadLog};


#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub enum TransactionTypes {
    Read,
    Write,
    ReadFile,
    DisplayDf
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
pub struct TransactionManager {
    pub active_transactions: HashMap<usize, Transaction>,
    pub lock_manager: LockManager,
    pub wal: Arc<Mutex<WriteAheadLog>>,
    pub database: HashMap<TransactionTypes, Message>,
}

impl TransactionManager {
    pub fn new(wal_path: &str) -> Self {
        TransactionManager {
            active_transactions: HashMap::new(),
            lock_manager: LockManager::new(),
            wal: Arc::new(Mutex::new(WriteAheadLog::new(wal_path).expect("Failed to initialize WAL"))),
            database: HashMap::new(),
        }
    }

    pub async fn start_transaction(&mut self, transaction_request: Message, node_topology: Vec<String>) -> Result<usize, ClusterExceptions> {
        let id = self.active_transactions.len() as usize + 1;
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

    pub async fn recover_from_wal(transaction_manager: &mut TransactionManager, wal_path: &str) {
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
    FailedToCommitTransaction { error_message: String }
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
        }
    }
}
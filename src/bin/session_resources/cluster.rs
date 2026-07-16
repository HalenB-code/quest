use crate::session_resources::message::{Message, MessageFields, MessageStatus, MessageType, MsgKind};
use std::collections::{HashMap, BTreeMap};
use crate::session_resources::node::{Node, NodeHandle};
use crate::session_resources::implementation::MessageExecutionType;
use crate::session_resources::messenger::Messenger;
use crate::session_resources::exceptions::ClusterExceptions;
use crate::session_resources::config::ClusterConfig;
use crate::session_resources::transactions::{TransactionManager, TransactionExceptions};
use crate::session_resources::file_system::FileSystemManager;
use crate::session_resources::network::{NetworkManager, NodeType};
use crate::session_resources::message;
use crate::session_resources::implementation::StdOut;
use crate::session_resources::message::{message_serializer, message_deserializer};
use crate::session_resources::write_ahead_log::{WriteAheadLog, WalEntry};
use crate::session_resources::transactions::Transaction;
use crate::session_resources::transactions::Action;

use tokio::sync::{mpsc};
use std::fmt::Debug;
use std::fs::File;
use std::io::Write;
use std::io;
use tokio::time::{timeout, Duration};

use super::message::MessageTypeFields;

// const WAL_PATH: &str = r"C:\rust\projects\rust-bdc";
const OVERWRITE_INCOMING_MSG_ID: bool = false;

// Cluster Class
// Collection of nodes that will interact to achieve a task
#[derive(Debug)]
pub struct Cluster {
    pub cluster_id: String,
    pub execution_target: MessageExecutionType,
    pub messenger: Messenger,
    pub nodes: BTreeMap<String, NodeHandle>,
    pub node_event_sender: mpsc::Sender<Message>,
    pub node_request_senders:HashMap<String, mpsc::Sender<Message>>,
    pub transaction_manager: Option<TransactionManager>,
    pub cluster_configuration: ClusterConfig,

    pub network_manager: NetworkManager,
    pub node_message_log: HashMap<usize, (Message, String)>
}

impl Cluster {
  pub async fn create(cluster_reference: usize, cluster_config: &ClusterConfig, network_manager: NetworkManager, messenger: Messenger, node_event_sender: mpsc::Sender<Message>, execution_target: MessageExecutionType, establish_network: bool) -> Self {

    let wal_path = &cluster_config.working_directory.wal_path;
    let local_working_directory = &cluster_config.working_directory.local_path;
    let file_system_accessibility = &cluster_config.working_directory.file_system_type;
    let mut nodes = BTreeMap::new();

    // Insert master node
    let (request_tx, _request_rx) = mpsc::channel::<Message>(100);
    let node_handle = NodeHandle::new("node-master".to_string(), request_tx);
    nodes.insert("node-master".to_string(), node_handle);

    let mut cluster: Cluster = Cluster {
      cluster_id: format!("cluster-{cluster_reference}"), 
      execution_target,
      messenger,
      nodes,
      node_event_sender: node_event_sender,
      node_request_senders: HashMap::new(),
      transaction_manager: None,
      cluster_configuration: cluster_config.clone(),
      network_manager,
      node_message_log: HashMap::new()
    };

    let transaction_manager = TransactionManager::new(wal_path.clone().as_str(), file_system_accessibility, local_working_directory);
    cluster.transaction_manager = Some(transaction_manager);
    return cluster;

  }

    pub async fn start_response_listener(&self, node_id: String) {

        let reader = match self
            .network_manager
            .network_response_readers
            .get(&node_id)
        {
            Some(r) => r.clone(),
            None => return,
        };

        let messenger = self.messenger.clone();

        tokio::spawn(async move {
            let mut lock = reader.lock().await;

            while let Some(messages) = lock.recv().await {
                for msg in messages {
                    if let Err(e) = messenger.response_queue(msg).await {
                        println!(
                            "Failed to enqueue response for {}: {:?}",
                            node_id, e
                        );
                    }
                }
            }

            println!("Response listener closed for {}", node_id);
        });
    }


    pub async fn process_initialization(&mut self, init_request_message: Message) -> Result<(), ClusterExceptions> {

        let node_exists = self.nodes.contains_key(init_request_message.node_id().unwrap());

        match node_exists {
            false => {
                let (request_tx, request_rx) = mpsc::channel::<Message>(100);
                let mut node = Node::create(&init_request_message, &self.cluster_configuration.working_directory.local_path, request_rx, self.node_event_sender.clone()).await;
                let node_handle = NodeHandle::new(node.node_id.clone(), request_tx.clone());
                let node_id = node.node_id.clone();
                self.network_manager.network_map.insert(node_id.clone(), ("".to_string(), NodeType::Local));

                // Store the new node in the cluster
                self.nodes
                    .entry(node_id.clone())
                    .or_insert(node_handle);

                self.node_request_senders.insert(node_id.clone(), request_tx);

                let node_check = self.nodes.contains_key(init_request_message.node_id().unwrap());
                
                if node_check {
                    println!("Node {} has been connected to {}", &node_id, self.cluster_id);
                    let message_execution_target = self.execution_target.clone();

                    tokio::spawn(async move {
                        if let Err(err) = node
                            .process_requests(message_execution_target)
                            .await
                        {
                            eprintln!(
                                "Error processing requests for node {}: {:?}",
                                node_id, err
                            );
                        }
                    });
                } else {
                    return Err(ClusterExceptions::FailedToRetrieveNodeFromCluster {
                        error_message: node_id,
                    });
                }
            }
            true => {
                if let Some((_key, _value)) = self.nodes.get_key_value(init_request_message.node_id().unwrap()) {
                    println!(
                        "Node {} already exists and is connected to {}",
                        init_request_message.node_id().unwrap(),
                        self.cluster_id
                    );
                } else {
                    return Err(ClusterExceptions::FailedToRetrieveNodeFromCluster {
                        error_message: init_request_message.node_id().unwrap().to_string(),
                    });
                }
            }
        }

    Ok(())
    }

    pub fn remove(mut self, node: Node) -> Result<(), ClusterExceptions> {

    let node_removal = self.nodes.remove(&node.node_id);

    match node_removal {
        Some(_node_removal) => {
        println!("{} has been disconnected from {}", node.node_id, self.cluster_id);
        },
        None => {
        return Err(ClusterExceptions::FailedToRemoveNodeFromCluster { error_message: node.node_id } );
        }
    }
    Ok(())
    }

    pub fn terminate(mut self) -> Result<(), ClusterExceptions> {
    let node_count = self.nodes.is_empty();

    match node_count {
        true => Err(ClusterExceptions::ClusterDoesNotHaveAnyNodes { error_message: self.cluster_id } ),
        false => {
        for key in &self.get_nodes() {
            println!("Removing {} from {}", key, self.cluster_id);
            self.nodes.remove(&key.clone());
        }
        println!("All nodes removed from {:?}", self.cluster_id);
        drop(self);
        Ok(())
        }
    }
    }

    pub fn get_nodes(&self) -> Vec<String> {
        // Exclude node-master as it is technically not an executable node
        // TODO: Add configuration to activate node-master as processing node on client machine
        self.nodes.keys().cloned().filter(|x| x != &"node-master".to_string()).collect()
    }

    pub fn count_nodes(&self) -> usize {
    self.nodes.len()
    }

    pub fn get_node_metadata(&self, node: String) -> Result<&NodeHandle, ClusterExceptions> {

        if let Some(node) = self.nodes.get(&node) {
            Ok(node)
        }
        else {
            Err(ClusterExceptions::NodeDoesNotExist { error_message: node.clone() })
        }
    }

    pub fn log_messages(&self) -> std::io::Result<()> {

        let file_path = format!("{}\\cluster_{}_log.txt", self.cluster_configuration.working_directory.local_path, self.cluster_id);

        let mut out_file = File::create(file_path)?;

        for (id, (message, status)) in self.node_message_log.iter() {
            out_file.write(format!("{} {:?} {}\n", id, message, status).as_bytes())?;
        }

        Ok(())
        }

    pub fn cluster_response(&self, message_response: Message) -> Result<(), ClusterExceptions> {
        
        if let MessageExecutionType::StdOut = self.execution_target {
        let serialized_response = message::message_serializer(&message_response)?;
        let stdout_lock = io::stdout().lock();
        StdOut::write_to_std_out(stdout_lock, format!("Response {}", serialized_response))?;
        return Ok(());
        } else {
        return Err(ClusterExceptions::InvalidClusterRequest { error_message_1: message_response, error_message_2: "StdOut".to_string() });
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
            },
            MessageStatus::Sent => {
                self.node_message_log
                .entry(message_id)
                .and_modify(|(_key, value)| {
                    *value = MessageStatus::Sent.to_string();
                });
            },
            _ => {
            return Err(ClusterExceptions::MessageStatusNotUpdated {
                error_message: message_id.to_string(),
            })
            }
        }

        Ok(())
    }

    pub async fn check_node_registration(&mut self, node_id: &String) -> Result<(), ClusterExceptions> {
        if self.nodes.contains_key(node_id) {
            Ok(())
        } else {
            let message_id = self.node_message_log.keys().max().unwrap_or(&0_usize) + 1;
            let other_nodes = self.nodes.keys().cloned().collect::<Vec<String>>();
            let node_init_message = message_deserializer(&format!(r#"{{"type":"init","msg_id":{message_id},"node_id":"{node_id}","node_ids":{:?}}}"#, other_nodes))?;
            
            if let Err(error) = self.process_local(message_id, node_init_message).await {
                return Err(ClusterExceptions::InvalidCommand { error_message: format!("Failed to deserialize init message for node: {}", node_id) });
            }
            else {
                return Ok(());
            }
        }
    }

    pub async fn categorize_and_queue(&mut self, incoming_message: String) -> Result<(), ClusterExceptions> {
        println!("Incoming message {}", incoming_message.clone());
        
        // Mapping incoming string to ClusterCommand type
        // Intention here is to be able to receive command line inputs that can be parsed and configured into the corresponding message (read RPC type) for internal use
        // let mapped_message = ClusterCommand::map_command_line_request(incoming_message)?;
        let mut mapped_message = message_deserializer(&incoming_message)?;
        let message_id = self.insert_message_log(mapped_message.clone()).await?;

        if OVERWRITE_INCOMING_MSG_ID {
                mapped_message.set_msg_id(message_id);
        }

        match mapped_message.kind() {
            
            MsgKind::Control => {
                // On the control plane, we are currently accommodating log cluster messages and initialization messages.
                // Log messages will write to std out, and init will trigger the cluster initialization that will have follow-up requests for things like network and file system setup
                match mapped_message.msg_type().unwrap().as_str() {
                    "log_cluster_messages" => {
                        if let Ok(()) = self.log_messages() {
                            let message_response = Message::Response {
                            src: mapped_message.dest().unwrap().to_string(),
                            dest: mapped_message.src().unwrap().to_string(),
                            body: MessageType::LogClusterMessagesOk {
                            }
                        };
                            self.cluster_response(message_response)?;
                        }            
                    },
                    "remote_connect_ok" => {
                        self.check_node_registration(&mapped_message.src().unwrap().to_string()).await?;
                        self.cluster_response(mapped_message)?;
                    },
                    "init" => {
                    self.process_local(message_id, mapped_message.clone()).await?;
                    }
                    _ => {
                    }
                };

                // While in this block, allow for any follow-ups to be processed
                self.process_followups().await?;
                return Ok(());
            },
            MsgKind::Task => {
                // Here we are dealing with messages that are on the data plane
                // These may be local or remote and should be routed accordingly

                if let Some(_destination) = mapped_message.dest() {

                    // Need to differentiate between response oks and response requests
                    // TODO: Change unwrap to proper error handling
                    match mapped_message.body().unwrap().is_ok() {
                        true => {
                            self.update_message_log(message_id, MessageStatus::Ok).await?;
                            return Ok(());
                        },
                        false => {
                            if let Some((_node, node_type)) = self.network_manager.network_map.get(mapped_message.dest().unwrap()) {
                            match node_type {
                                NodeType::Local => {
                                self.process_local(message_id, mapped_message.clone()).await?;
                                },
                                NodeType::Remote => {
                                self.process_remote(message_id, mapped_message.clone()).await?;
                                }
                            }
                            } else {
                                // If request is init or another type, we don't need confirmation node exists
                                self.update_message_log(message_id, MessageStatus::Failed).await?;
                                return Err(ClusterExceptions::InvalidCommand { error_message: format!("Invalid command for node: {}", mapped_message.dest().unwrap().clone()) });
                            }
                        }
                    }
                } else {
                    // If request is init or another type, we don't need confirmation node exists
                    self.update_message_log(message_id, MessageStatus::Failed).await?;
                    return Err(ClusterExceptions::NodeDoesNotExist { error_message: mapped_message.dest().unwrap().clone() });
                };
                self.process_followups().await?;
                return Ok(());
            }
        }
    }
        
    pub async fn process_local(&mut self, message_id: usize, message_request: Message) -> Result<(), ClusterExceptions> {

        println!("Processing local message: {:?}", message_request);

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
                let message_type = message_request
                    .msg_type()
                    .ok_or_else(|| {
                        ClusterExceptions::InvalidCommand {
                            error_message:
                                "Request missing message type"
                                    .to_string(),
                        }
                    })?
                    .to_string();

                match message_type.as_str() {
                    "txn" => {
                        match self
                            .categorize_and_queue_transactions(
                                message_request,
                            )
                            .await
                        {
                            Ok(()) => {
                                self.update_message_log(
                                    message_id,
                                    MessageStatus::Ok,
                                )
                                .await?;

                                Ok(())
                            }

                            Err(error) => {
                                self.update_message_log(
                                    message_id,
                                    MessageStatus::Failed,
                                )
                                .await?;

                                Err(error)
                            }
                        }
                    }

                    _ => {
                        self.send_request_to_node(
                            message_id,
                            message_request,
                        )
                        .await
                    }
                }
            },
            Message::Response { .. } => {
                // TODO: What do we do with responses?
                // self.handle_response(message_request).await?;
                self.update_message_log(message_id, MessageStatus::Ok).await?;
                Ok(())
            }
        }
    }

    pub async fn process_remote(
        &mut self,
        message_id: usize,
        message_request: Message
    ) -> Result<(), ClusterExceptions> {

        let node_id = message_request.dest()
            .ok_or_else(|| ClusterExceptions::InvalidCommand {
                error_message: "Remote message missing dest".into()
            })?;

        let sender = self.network_manager
            .network_request_writers
            .get(node_id)
            .ok_or_else(|| ClusterExceptions::RemoteNodeRequestError {
                error_message: "no sending channel".into()
            })?;

        sender.send(message_request.clone()).await
            .map_err(|e| ClusterExceptions::RemoteNodeRequestError {
                error_message: e.to_string()
            })?;

        // Mark as SENT — completion will happen when responses flow in
        println!("{:?}", self.node_message_log.clone());
        self.update_message_log(message_id, MessageStatus::Sent).await?;

        Ok(())
    }

    #[deprecated = "Use async response listener instead"]
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

    pub async fn _process_remote(&mut self, message_id: usize, message_request: Message) -> Result<(), ClusterExceptions> {

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
        let transaction_id: usize;
        let transaction_actions: Vec<Action>;

        if let Some(transaction_manager) = &mut self.transaction_manager {
            transaction_id = transaction_manager.start_transaction(message_request.clone(), node_topology).await?;
            transaction_actions = transaction_manager.execute_transaction(message_request.dest().unwrap().clone(), transaction_id).await?;
        } else {
            return Err(ClusterExceptions::TransactionError(TransactionExceptions::TransactionManagerNotInitialized { error_message: "Transaction manager not initialized".to_string() }));
        }
        let mut transaction_execution_error = None;

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
                        transaction_execution_error = Some(());
                        }
                    };
                    },
                    Err(_error) => {
                    transaction_execution_error = Some(());
                    }
                } 
            } else {
                transaction_execution_error = Some(());
            }
            } else {
                transaction_execution_error = Some(());
            }
        };

        if let Some(transaction_manager) = &mut self.transaction_manager {

            if let None = transaction_execution_error {
                transaction_manager.commit_transaction(transaction_id).await?;
                self.update_message_log(message_id, MessageStatus::Ok).await?;
                return Ok(())
            } else {
                self.update_message_log(message_id, MessageStatus::Failed).await?;
                return Err(ClusterExceptions::TransactionError(TransactionExceptions::FailedToCommitTransaction { error_message: transaction_id.to_string() }));
            }

        } else {
            return Err(ClusterExceptions::TransactionError(TransactionExceptions::TransactionManagerNotInitialized { error_message: "Transaction manager not initialized".to_string() }));
        }

    }

    pub async fn complete_transaction(&mut self, message_request: Message) -> Result<(), ClusterExceptions> {

        let node_topology = self.get_nodes();

        if let Some(transaction_manager) = &mut self.transaction_manager {

            let transaction_id = transaction_manager.start_transaction(message_request.clone(), node_topology).await?;
            let transaction_actions = transaction_manager.execute_transaction(message_request.dest().unwrap().clone(), transaction_id).await?;

            let mut transaction_execution_error = None;

            for action in transaction_actions {

                if let Some(transaction_message_request) = action.value {
                if let Err(_error) = self.messenger.request_queue(transaction_message_request).await {
                    transaction_execution_error = Some(())
                }
                }

            };

            if let None = transaction_execution_error {
                transaction_manager.commit_transaction(transaction_id).await?;
                Ok(())
            } else {
                return Err(ClusterExceptions::TransactionError(TransactionExceptions::FailedToCommitTransaction { error_message: transaction_id.to_string() }));
            }

        } else {
            return Err(ClusterExceptions::TransactionError(TransactionExceptions::TransactionManagerNotInitialized { error_message: "Transaction manager not initialized".to_string() }));
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

        // Drain quickly under the lock
        let mut drained = Vec::new();
        {
            let mut lock = cluster_messenger.message_responses.lock().await;
            while let Some(m) = lock.pop_front() {
                drained.push(m);
            }
        } // lock dropped here

        // Now process without holding the lock
        for msg in drained {
            match msg {
                Message::Init { .. } => self.process_initialization(msg).await?,
                Message::Request { .. } => self.messenger.request_queue(msg).await?,
                Message::Response { .. } => {
                    // TODO: DO NOT error: route to response handler
                    // self.handle_response(msg).await?;
                }
            }
        }
        Ok(())
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

    pub async fn send_request_to_node(&mut self, message_id: usize, message_request: Message) -> Result<(), ClusterExceptions> {
        let destination = message_request
            .dest()
            .ok_or_else(|| {
                ClusterExceptions::InvalidCommand {
                    error_message:
                        "Node request missing destination"
                            .to_string(),
                }
            })?
            .to_string();

        let sender = self
            .node_request_senders
            .get(&destination)
            .ok_or_else(|| {
                ClusterExceptions::NodeDoesNotExist {
                    error_message: destination.clone(),
                }
            })?
            .clone();

        match sender.send(message_request).await {
            Ok(()) => {
                self.update_message_log(
                    message_id,
                    MessageStatus::Sent,
                )
                .await?;

                Ok(())
            }

            Err(error) => {
                self.update_message_log(
                    message_id,
                    MessageStatus::Failed,
                )
                .await?;

                Err(
                    ClusterExceptions::InvalidCommand {
                        error_message: format!(
                            "Failed to send request to \
                            {destination}: {error}"
                        ),
                    },
                )
            }
        }
    }

}

#[derive(Debug)]
pub struct ClusterContext {
  pub cluster_config: ClusterConfig,
  pub nodes: Vec<String>,
  pub file_system_manager: FileSystemManager,
}

impl ClusterContext {
    pub fn new(cluster_config: ClusterConfig, nodes: Vec<String>, fs_manager: FileSystemManager) -> Self {
        ClusterContext {
            cluster_config,
            nodes,
            file_system_manager: fs_manager,
        }
    }

    pub fn add_node(&mut self, node_id: String) {
        if !self.nodes.contains(&node_id) {
            self.nodes.push(node_id);
        }
    }

    pub fn remove_node(&mut self, node_id: &str) {
        self.nodes.retain(|existing| existing != node_id);
    }

}
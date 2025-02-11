use crate::session_resources::message::{Message, MessageType, MessageFields, MessageStatus};
use std::collections::{HashMap, BTreeMap};
use std::str::ParseBoolError;
use crate::session_resources::node::Node;
use crate::session_resources::implementation::MessageExecutionType;
use crate::session_resources::message::message_serializer;
use crate::session_resources::messenger::Messenger;
use crate::session_resources::exceptions::ClusterExceptions;
use crate::session_resources::config::ClusterConfig;
use crate::session_resources::transactions::{TransactionManager, TransactionExceptions};
use crate::session_resources::file_system::FileSystemManager;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use std::fmt::Debug;
use std::fs::File;
use std::io::Write;
use tokio::time::{sleep, Duration};

// const WAL_PATH: &str = r"C:\rust\projects\rust-bdc";
const OVERWRITE_INCOMING_MSG_ID: bool = true;

// Cluster Class
// Collection of nodes that will interact to achieve a task
#[derive(Debug, Clone)]
pub struct Cluster {
    pub cluster_id: String,
    pub receiving_channel: Arc<Mutex<mpsc::Receiver<String>>>,
    pub execution_target: MessageExecutionType,
    pub messenger: Messenger,
    pub nodes: BTreeMap<String, Node>,
    pub node_message_log: HashMap<usize, (Message, String)>,
    pub transaction_manager: TransactionManager,
    pub cluster_configuration: ClusterConfig,
    pub file_system_manager: FileSystemManager,
  }
  
  impl Cluster {
    pub fn create(
      cluster_reference: usize, 
      incoming_message_handler: mpsc::Receiver<String>,
      execution_target: MessageExecutionType,
      source_path: String,
    ) -> Self {

      let cluster_config = ClusterConfig::read_config(source_path);
      let wal_path = &cluster_config.working_directory.wal_path;
      let file_system_accessibility = &cluster_config.working_directory.file_system_type;

      Self { 
        cluster_id: format!("cluster-{cluster_reference}"), 
        receiving_channel: Arc::new(Mutex::new(incoming_message_handler)),
        execution_target,
        messenger: Messenger::create(), 
        nodes: BTreeMap::new(), 
        node_message_log: HashMap::new(),
        transaction_manager: TransactionManager::new(wal_path.clone().as_str()),
        cluster_configuration: cluster_config.clone(),
        file_system_manager: FileSystemManager::new(file_system_accessibility)
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

    pub async fn process_initialization(&mut self, init_request_message: Message) -> Result<(), ClusterExceptions> {

      let node_exists = self.nodes.contains_key(init_request_message.node_id().unwrap());

      match node_exists {
          false => {
              let mut node = Node::create(&init_request_message).await;
              let node_id = node.node_id.clone();
  
              // Store the new node in the cluster
              self.nodes
                  .entry(node_id.clone())
                  .or_insert(node.clone());
  
              let node_check = self.nodes.contains_key(init_request_message.node_id().unwrap());
              
              if node_check {
                  println!("Node {} has been connected to {}", &node_id, self.cluster_id);
  
                  let cluster_arc = Arc::new(Mutex::new(self.clone()));
                  let message_execution_target = self.execution_target.clone();
  
                  tokio::spawn(async move {
                    let cluster_clone = Arc::clone(&cluster_arc);
                      if let Err(err) = node
                          .process_requests(message_execution_target, &cluster_clone)
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
  
    pub async fn categorize_and_queue(&mut self, incoming_message: String) -> Result<(), ClusterExceptions> {

      let mapped_message = self.map_request(incoming_message)?;
      let mut message_request = self.messenger.categorize(mapped_message).await?;
      let message_id = self.insert_message_log(message_request.clone()).await?;

      if OVERWRITE_INCOMING_MSG_ID {
        message_request.set_msg_id(message_id);
      }

      match message_request {
        Message::Init { .. } => {
            if let Err(_error) = self.process_initialization(message_request).await {
              self.update_message_log(message_id, MessageStatus::Failed).await?;
            } else {
              self.update_message_log(message_id, MessageStatus::Ok).await?;
            };
        },
        Message::Request { .. } => {
          match message_request.msg_type().unwrap().as_str() {
            "txn" => {
              if let Err(_error) = self.categorize_and_queue_transactions(message_request).await {
                self.update_message_log(message_id, MessageStatus::Failed).await?;
              } else {
                self.update_message_log(message_id, MessageStatus::Ok).await?;
              };
            },
            _ => {
              
              if let Err(_error) = self.messenger.request_queue(message_request).await {
                self.update_message_log(message_id, MessageStatus::Failed).await?;
              } else {
                self.update_message_log(message_id, MessageStatus::Ok).await?;
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

      Ok(())
  }

    pub async fn complete_transaction(&mut self, message_request: Message) -> Result<(), ClusterExceptions> {

    let node_metadata = self.get_node_metadata(message_request.dest().unwrap().clone())?;
    let node_topology = node_metadata.other_node_ids.clone();

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

    // self.abort
    // self.roll_back
    // self.recover

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

        let cluster_clone = self.clone();
        let mut messenger_request_queue_lock = cluster_clone.messenger.message_responses.lock().await;
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

    pub async fn run(&mut self) -> Result<(), ClusterExceptions> {
      
      // Clone the Arc for the receiving channel
      let receiver_clone = Arc::clone(&self.receiving_channel);

      // Lock the receiver (this avoids the temporary value issue)
      let mut rx = receiver_clone.lock().await;
      
      while let Some(incoming_message) = rx.recv().await {
        self.categorize_and_queue(incoming_message).await?;
        // Process follow-ups with a slight delay if none exist
        loop {
          if self.messenger.message_responses.lock().await.is_empty() {
              break;
          }
          self.process_followups().await?;
          sleep(Duration::from_millis(1)).await; // Small delay to prevent excessive locking
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
      self.nodes.keys().cloned().collect()
    }

    pub fn count_nodes(&self) -> usize {
    self.nodes.len()
    }

    pub fn get_node_metadata(&self, node: String) -> Result<&Node, ClusterExceptions> {

    let node_fetch = self.nodes.get(&node);

    if let Some(node_metadata) = node_fetch {
      Ok(node_metadata)
    }
    else {
      Err(ClusterExceptions::NodeDoesNotExist { error_message: node.clone() })
    }

    }

    pub fn read_data_from_file(&mut self, file_path: String, delimiter: Option<String>) -> Result<String, ClusterExceptions> {

    // Receive read request message
    // File path retrieved from request
    // Nodes are retrieved from cluster
    // File path is added to file manager record
    // Byte ordinals are generated for file based on nodes
    // Transaction message is created that has a read request for each node with ordinal positions
    // Depending on the FileSystemType, read is either receive bytes from home client and write to local file
    // or, read existing file on distributed file share
    // Read transaction completed or failed

    // Get nodes
    let nodes = self.get_nodes();
    let separator: u8;
    
    if let Some(delim) = delimiter {
      separator = delim.as_bytes()[0];
    } else {
      separator = ",".to_string().as_bytes()[0];
    }

    // Inserts hash of file path into FileSystemManager
    if let Ok(file_path_hash) = self.file_system_manager.read_from_file(file_path.clone(), &nodes) {
      
      // Open new transaction
      if let Some(ref mut transaction) = Message::default_request_message(MessageType::Transaction { txn: vec![] } ) {
        transaction.set_src("cluster-orch".to_string());

        // TODO
        // This will pull Node from first element in vector which is not guaranteed to be n1
        // nodes can also be empty
        transaction.set_dest(nodes[0].clone());

        let file_system_type = self.cluster_configuration.working_directory.file_system_type.clone();

        let infered_file_schema = FileSystemManager::get_file_header(file_path.clone(), separator)?;
        let infered_file_schema_string = serde_json::to_string(&infered_file_schema)?;
        let byte_ordinals = FileSystemManager::get_byte_ordinals(file_path.clone(), &nodes)?;
        let byte_ordinals_string = serde_json::to_string(&byte_ordinals)?;
        
        if let Some(_file_object) = self.file_system_manager.files.get(&file_path_hash) {
          transaction.set_body(MessageType::Transaction { txn: vec![vec!["rf".to_string(), file_path.clone(), file_system_type.to_string(), byte_ordinals_string, infered_file_schema_string]] });
        } else {
          return Err(ClusterExceptions::UnkownClientRequest { error_message: file_path });
        }

        if let Ok(request_string) = message_serializer(&transaction) {
          return Ok(request_string);
        } else {
          return Err(ClusterExceptions::UnkownClientRequest { error_message: file_path });
        }

      } else {
        return Err(ClusterExceptions::UnkownClientRequest { error_message: file_path });
      }
    } else {
    return Err(ClusterExceptions::UnkownClientRequest { error_message: file_path });
    }

    }

    pub fn display_df(&self, df_name: String) -> Result<String, ClusterExceptions> {
    // TODO
    // Harcoding name for "df" for now

    // TODO
    // All nodes or not?
    let all_nodes: bool = false;

    let nodes = self.get_nodes();

    // TODO
    // Move this to transaction to cater for multiple node displays without having to handle multiple display requests at the session/cluster level
    let message_request = Message::Request { 
      src: "cluster-orch".to_string(), 
      dest: nodes[0].to_string(), 
      body: MessageType::DisplayDf { 
        df_name, 
        total_rows: 5
      } 
    };

    println!("{:?}", message_request);
    return Ok(message_serializer(&message_request)?);


    }

    pub fn map_request(&mut self, request: String) -> Result<String, ClusterExceptions> {

      let args: Vec<String> = request.split(" ").map(|element| element.to_string()).collect();
      // println!("{:?}", args);
      let mut action = None;
      let mut file_path = None;
      let mut df_name = None;
      let mut delimiter: Option<String> = None;

      let mut iter = args.iter().peekable();
      while let Some(arg) = iter.next() {
          match arg.as_str() {
              "-a" => action = iter.next().map(|s| s.clone()),
              "-fp" => file_path = iter.next().map(|s| s.clone()),
              "-df_name" => df_name = iter.next().map(|s| s.clone()),
              "-delimiter" => delimiter = iter.next().map(|s| s.clone()),
              _ => {}
          }
      }

      if let Some(parsed_action) = action.clone() {
        match parsed_action.as_str() {
          "read-file" => {
            // TODO
            // Need to accept df name when read request received
            if let Some(fp) = file_path {
              let request_string = self.read_data_from_file(fp.to_string(), delimiter)?;
              return Ok(request_string);
            } else {
              return Err(ClusterExceptions::NodeDoesNotExist { error_message: request });
            }
          },
          "display-df" => {
            if let Some(df_name) = df_name {
              let request_string = self.display_df(df_name.to_string())?;
              return Ok(request_string);
            } else {
              return Err(ClusterExceptions::NodeDoesNotExist { error_message: request });
            }
          },
          "log" => {
            // TODO
            // Need to accept log level when log request received
            return Ok("log".to_string());
          },
          _ => {
              return Ok(request);
          }
        }
      } else {
      return Ok(request);
      }


    }

    pub fn log_messages(cluster: &Cluster) -> std::io::Result<()> {

      let file_path = format!("{}\\cluster_{}_log.txt", cluster.cluster_configuration.working_directory.local_path, cluster.cluster_id);

      let mut out_file = File::create(file_path)?;

      for (id, (message, status)) in cluster.node_message_log.iter() {
        out_file.write(format!("{} {:?} {}\n", id, message, status).as_bytes())?;
      }

      Ok(())
    }

}
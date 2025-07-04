use crate::session_resources::message::{Message, MessageType, MessageFields, MessageStatus};
use std::collections::{HashMap, BTreeMap};
use crate::session_resources::node::Node;
use crate::session_resources::implementation::MessageExecutionType;
use crate::session_resources::message::message_serializer;
use crate::session_resources::messenger::Messenger;
use crate::session_resources::exceptions::ClusterExceptions;
use crate::session_resources::config::ClusterConfig;
use crate::session_resources::transactions::{TransactionManager, TransactionExceptions};
use crate::session_resources::file_system::FileSystemManager;
use crate::session_resources::network::{self, NetworkManager, NodeType};
use crate::session_resources::message;
use crate::session_resources::implementation::StdOut;

use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use std::fmt::Debug;
use std::fs::File;
use std::io::Write;
use std::io;
use tokio::time::{timeout, Duration};
use tokio::sync::mpsc::Receiver;

use super::message::MessageTypeFields;

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
    pub network_manager: NetworkManager
}
  
impl Cluster {
  pub async fn create(cluster_reference: usize, outgoing_message_handler: mpsc::Sender<String>, incoming_message_handler: mpsc::Receiver<String>, execution_target: MessageExecutionType, source_path: String, establish_network: bool) -> Self {

    let cluster_config = ClusterConfig::read_config(source_path);
    let wal_path = &cluster_config.working_directory.wal_path;
    let file_system_accessibility = &cluster_config.working_directory.file_system_type;
    let network_layout = &cluster_config.network;

    let network_manager = NetworkManager::new(network_layout, outgoing_message_handler, cluster_config.working_directory.local_path.clone(), establish_network);
    let mut nodes = BTreeMap::new();
    let other_nodes = network_manager.network_map.clone().into_keys().collect::<Vec<String>>();
    let network_map = network_manager.network_map.clone();

    if establish_network {

      for (node_id, (node_port, node_type)) in network_map {
        match node_type {
          NodeType::Local => {
          },
          NodeType::Remote => {
            let init_request = message::message_deserializer(&format!(r#"{{"type":"init","msg_id":0,"node_id":"{node_id}","node_ids":{:?}}}"#, other_nodes)).expect("Expecting correct init request structure");
            let virtual_remote_node = Node::create(&init_request, &cluster_config.working_directory.local_path.clone()).await;
            nodes.insert(node_id.to_string(), virtual_remote_node);
          }
        }
      }
      
    }

    Self {
      cluster_id: format!("cluster-{cluster_reference}"), 
      receiving_channel: Arc::new(Mutex::new(incoming_message_handler)),
      execution_target,
      messenger: Messenger::create(), 
      nodes,
      node_message_log: HashMap::new(),
      transaction_manager: TransactionManager::new(wal_path.clone().as_str()),
      cluster_configuration: cluster_config.clone(),
      file_system_manager: FileSystemManager::new(file_system_accessibility),
      network_manager
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
            let mut node = Node::create(&init_request_message, &self.cluster_configuration.working_directory.local_path).await;
            let node_id = node.node_id.clone();
            self.network_manager.network_map.insert(node_id.clone(), ("".to_string(), NodeType::Local));

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



  pub async fn run(&mut self) -> Result<(), ClusterExceptions> {
    
    // Clone the Arc for the receiving channel
    let receiver_clone = Arc::clone(&self.receiving_channel);

    // Lock the receiver (this avoids the temporary value issue)
    let mut rx = receiver_clone.lock().await;
    
    while let Some(incoming_message) = rx.recv().await {
      self.categorize_and_queue(incoming_message).await?;
      // Process follow-ups with a slight delay if none exist
      self.process_followups().await?;
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

  if let Some(node) = self.nodes.get(&node) {
    Ok(node)
  }
  else {
    Err(ClusterExceptions::NodeDoesNotExist { error_message: node.clone() })
  }

  }

  pub fn map_request(&mut self, request: String) -> Result<String, ClusterExceptions> {

    let args: Vec<String> = request.split(" ").map(|element| element.to_string()).collect();
    // println!("{:?}", args);
    let mut action = None;
    let mut file_path = None;
    let mut df_name = None;
    let mut delimiter: Option<String> = None;
    let mut target_node: Option<String> = None;
    let mut all_nodes: bool = false;

    let mut iter = args.iter().peekable();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "-a" => action = iter.next().map(|s| s.clone()),
            "-fp" => file_path = iter.next().map(|s| s.clone()),
            "-df_name" => df_name = iter.next().map(|s| s.clone()),
            "-delimiter" => delimiter = iter.next().map(|s| s.clone()),
            "-target-node" => target_node = iter.next().map(|s| s.clone()),
            "-all-nodes" => all_nodes = true,
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
            let request_string = self.display_df(df_name.to_string(), target_node, all_nodes)?;
            return Ok(request_string);
          } else {
            return Err(ClusterExceptions::NodeDoesNotExist { error_message: request });
          }
        },
        "group-by" => {
          if let Some(df_name) = df_name {
            let request_string = self.display_df(df_name.to_string(), target_node, all_nodes)?;
            return Ok(request_string);
          } else {
            return Err(ClusterExceptions::NodeDoesNotExist { error_message: request });
          }
        },
        "log-cluster" => {
          // TODO
          // Need to accept log level when log request received
          return Ok("log-cluster".to_string());
        },
        "log-node" => {
          // TODO
          // Need to accept log level when log request received
          return Ok("log-node".to_string());
        },
        _ => {
            return Ok(request);
        }
      }
    } else {
    return Ok(request);
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




}
use crate::session_resources::message::{Message, MessageType, MessageFields, MessageTypeFields};
use std::collections::{HashMap, BTreeMap};
use crate::session_resources::node::Node;
use crate::session_resources::implementation::MessageExecutionType;
use crate::session_resources::message::message_serializer;
use crate::session_resources::messenger::Messenger;
use crate::session_resources::exceptions::ClusterExceptions;
use crate::session_resources::warnings::ClusterWarnings;
use crate::session_resources::config::ClusterConfig;
use crate::session_resources::datastore::{ClusterDataStoreTypes, ClusterDataStoreObjects, ClusterDataStoreTrait, ClusterVector};
use crate::session_resources::transactions::{TransactionManager, TransactionExceptions};
use crate::session_resources::file_system::FileSystemManager;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use std::fmt::Debug;
use std::hash::Hash;

// const WAL_PATH: &str = r"C:\rust\projects\rust-bdc";

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
    pub datastore: HashMap<ClusterDataStoreTypes, Box<dyn ClusterDataStoreTrait>>,
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
        execution_target: execution_target,
        messenger: Messenger::create(), 
        nodes: BTreeMap::new(), 
        node_message_log: HashMap::new(),
        datastore: HashMap::new(),
        transaction_manager: TransactionManager::new(wal_path.clone().as_str()),
        cluster_configuration: cluster_config.clone(),
        file_system_manager: FileSystemManager::new(file_system_accessibility)
      }
    }

    pub async fn process_initialization(
      &mut self,
      init_request_message: Message
    ) -> Result<(), ClusterExceptions> {

      let node_exists = self.nodes.contains_key(init_request_message.node_id().unwrap());

      match node_exists {
          false => {
              let mut node = Node::create(&init_request_message).await;
              let node_id = node.node_id.clone();
              let nodes_id_ref = *self.node_message_log.keys().max().unwrap_or(&0_usize) + 1;
  
              // Store the new node in the cluster
              self.nodes
                  .entry(node_id.clone())
                  .or_insert(node.clone());

              self.node_message_log
                  .insert(nodes_id_ref, (init_request_message.clone(), "ok".to_string()));
  
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

    let message_request = self.messenger.categorize(incoming_message).await?;

    // TODO Assign msg_id based on global log and set status to out

    match message_request {
      Message::Init { .. } => {
          self.process_initialization(message_request).await?;
      },
      Message::Request { .. } => {
        self.messenger.request_queue(message_request).await?;
      },
      Message::Response { .. } => {
        return Err(ClusterExceptions::UnkownClientRequest {
          error_message: message_request.dest().unwrap().to_string(),
        });
      }
    }


    
    // TODO If no error comes back, set status to ok in global log

    Ok(())
}

async fn complete_transaction(&mut self, message_request: Message) -> Result<(), ClusterExceptions> {

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

pub async fn categorize_and_queue_transactions(&mut self, incoming_message: String) -> Result<(), ClusterExceptions> {

  let message_request = self.messenger.categorize(incoming_message).await?;

  // TODO Assign msg_id based on global log and set status to out

  match message_request {
    Message::Init { .. } => {
        self.process_initialization(message_request).await?;
    },
    Message::Request { .. } => {

      if let Some(typ) = message_request.msg_type() {
        match typ.as_str() {
          "txn" => {

            self.complete_transaction(message_request).await?;

          },
          _ => {
            self.messenger.request_queue(message_request).await?;
          }
        }
      }
      
    },
    Message::Response { .. } => {
      return Err(ClusterExceptions::UnkownClientRequest {
        error_message: message_request.dest().unwrap().to_string(),
      });
    }
  }


  
  // TODO If no error comes back, set status to ok in global log

  Ok(())
}

pub fn register_datastore<T>(&mut self, typ: ClusterDataStoreTypes, store: ClusterDataStoreObjects<T>)
where
    T: Clone + Eq + Hash + Send + Sync + 'static + Debug,
{
    self.datastore.insert(typ, Box::new(store));
}

pub async fn propagate_update(&mut self, message: Message) -> Result<(), ClusterExceptions> {
  match message.body() {
    Some( MessageType::VectorAdd { .. } ) => {
      let datastore_object_check = &self.datastore.contains_key(&ClusterDataStoreTypes::Vector);

      match datastore_object_check {
        false => {
          self.register_datastore(
            ClusterDataStoreTypes::Vector,
            ClusterDataStoreObjects::Vector(ClusterVector::<String>::create()),
        );
        
        },
        true => {
          ()
        }
      }
      // TODO
      // Here we are explicity tying String type to VectorAdd method
      if let Some(string) = message.body().unwrap().delta() {
        if let Some(store) = self.datastore.get(&ClusterDataStoreTypes::Vector) {
          if let Some(vector_store) = store.as_any().downcast_ref::<ClusterDataStoreObjects<String>>() {
            if let ClusterDataStoreObjects::Vector ( data ) = vector_store {
              let mut locked_data = data.data.lock().await;
              locked_data.push(string.clone())
            }
          }
        }
      }
    },
    _ => {
      return Err(ClusterExceptions::UnkownClientRequest {
        error_message: message.dest().unwrap().to_string(),
      });
    }
  }

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

pub async fn get_vector_store<T>(&self) -> Option<Arc<Mutex<Vec<T>>>>
where
    T: Clone + Eq + std::hash::Hash + Send + Sync + 'static + std::fmt::Debug,
{
    if let Some(store) = self.datastore.get(&ClusterDataStoreTypes::Vector) {
        // Downcast to ClusterDataStoreObjects<T>
        if let Some(concrete_store) = store.as_any().downcast_ref::<ClusterDataStoreObjects<T>>()
        {
            // Extract ClusterVector<T> and return its Arc<Mutex<Vec<T>>>
            if let ClusterDataStoreObjects::Vector(vector) = concrete_store {
                return Some(Arc::clone(&vector.data));
            }
        }
    }
    None
}

  pub async fn process_followups(&mut self) -> Result<(), ClusterExceptions> {

    let response_queue_check = self.messenger.message_responses.lock().await.is_empty();
    // println!("In the request response section {:?}", &self.messenger.message_responses.lock().await);

    if !response_queue_check {
      
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
    }

    Ok(())

  }

  pub async fn run(&mut self) -> Result<(), ClusterExceptions> {
    
    // Clone the Arc for the receiving channel
    let receiver_clone = Arc::clone(&self.receiving_channel);

    // Lock the receiver (this avoids the temporary value issue)
    let mut rx = receiver_clone.lock().await;
    
    while let Some(incoming_message) = rx.recv().await {

        // println!("{:?}", &message_request);
        self.categorize_and_queue(incoming_message).await?;
        self.process_followups().await?

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
  
    pub fn read_data_from_file(&mut self, file_path: String) -> Result<String, ClusterExceptions> {

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

      // Inserts hash of file path into FileSystemManager
      if let Ok(file_path_hash) = self.file_system_manager.read_from_file(file_path.clone(), &nodes) {

        // Open new transaction
        if let Some(ref mut transaction) = Message::default_request_message(MessageType::Transaction { txn: vec![] } ) {
          transaction.set_src("cluster-orch".to_string());

          // TODO
          // This will pull Node from first element in vector which is not guaranteed to be n1
          transaction.set_dest(nodes[0].clone());

          let file_system_type = self.cluster_configuration.working_directory.file_system_type.clone();

          if let Some(file_object) = self.file_system_manager.files.get(&file_path_hash) {
            transaction.set_body(MessageType::Transaction { txn: vec![vec!["rf".to_string(), file_path.clone(), file_system_type.to_string(), file_object.to_string_for("partition_ordinals").unwrap()]] });
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

}
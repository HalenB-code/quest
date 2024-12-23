use crate::session_resources::message::Message;
use std::collections::{HashMap, BTreeMap};
use crate::session_resources::node::Node;
use crate::session_resources::implementation::MessageExecutionType;
use crate::session_resources::message::{MessageType, MessageFields, MessageTypeFields};
use crate::session_resources::messenger::Messenger;
use crate::session_resources::exceptions::ClusterExceptions;
use crate::session_resources::warnings::ClusterWarnings;
use crate::session_resources::datastore::{ClusterDataStoreTypes, ClusterDataStoreObjects, ClusterDataStoreTrait, ClusterVector};
use crate::session_resources::transactions::{TransactionManager, TransactionExceptions};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use std::fmt::Debug;
use std::hash::Hash;

const WAL_PATH: &str = "";

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
    pub transaction_manager: TransactionManager
  }
  
  impl Cluster {
    pub fn create(
      cluster_reference: usize, 
      incoming_message_handler: mpsc::Receiver<String>,
      execution_target: MessageExecutionType,
    ) -> Self {

      Self { 
        cluster_id: format!("cluster-{cluster_reference}"), 
        receiving_channel: Arc::new(Mutex::new(incoming_message_handler)),
        execution_target: execution_target,
        messenger: Messenger::create(), 
        nodes: BTreeMap::new(), 
        node_message_log: HashMap::new(),
        datastore: HashMap::new(),
        transaction_manager: TransactionManager::new(WAL_PATH)
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
  
  }



//   use tokio::time::{sleep, Duration};
// use std::collections::{VecDeque, HashMap};
// // use anyhow::{anyhow, Result};

// #[derive(Debug)]
// struct Node {
//     id: usize,
//     nodes: Vec<String>,
//     msg_in: VecDeque<usize>,
//     msg_out: Vec<usize>
// }

// impl Node {
//     async fn say_hi(&self) {
//         println!("Hi from node {}", &self.id);
//     }

//     async fn process_msg(&mut self, msg: usize) {
    
//         self.msg_in.push_back(msg);
        
//         let msg_to_process = self.msg_in.pop_front();
        
//         match msg_to_process {
//             Some(usize) => {
            
//             self.msg_out.push(msg);
//             println!("Msg {} processed by {}", msg, self.id);
//             sleep(Duration::from_millis(10)).await;
                
//             },
//             None => { }
//         }
//     }
// }

// struct Cluster {
//     id: String,
//     nodes: HashMap<String, Node>
// }

// impl Cluster {
//     async fn communicate(&mut self, msg: usize) {
//         for (node_id, node) in &mut self.nodes {
//             node.say_hi().await;
//             node.process_msg(msg).await;
//         }
//     }
// }

// #[tokio::main]
// async fn main() {

//     let mut node_a = Node { id: 1, nodes: vec![2], msg_in: VecDeque::new() , msg_out: Vec::new() };
//     let mut node_b = Node { id: 2, nodes: vec![3], msg_in: VecDeque::new() , msg_out: Vec::new() };
//     let mut node_c = Node { id: 3, nodes: vec![1], msg_in: VecDeque::new() , msg_out: Vec::new() };
    
//     let mut cluster = Cluster { id: "cluster_a".to_owned(), nodes: HashMap::new() };
    
//     cluster.nodes.insert("n1".to_string() , node_a);
//     cluster.nodes.insert("n2".to_string() , node_b);
//     cluster.nodes.insert("n3".to_string() , node_c);

//     // for (node_id, node) in &cluster.nodes {
//     //     node.say_hi().await;
//     // };
    
//     let handle = tokio::spawn(async move {
    
//         for i in 0..5 {
//             cluster.communicate(i).await;
//         }
//         println!("{:?}", cluster.nodes.get("n1"));
//     });

//     let out = handle.await.unwrap();
    
    
    
//     println!("All done from main!");

// }


// V2

// use tokio::time::{sleep, Duration};
// use std::collections::{VecDeque, HashMap};
// // use anyhow::{anyhow, Result};

// #[derive(Debug)]
// struct Node {
//     id: usize,
//     nodes: Vec<String>,
//     msg_in: VecDeque<usize>,
//     msg_out: Vec<String>
// }

// impl Node {
//     async fn say_hi(&self) {
//         println!("Hi from node {}", &self.id);
//     }

//     async fn process_msg(&mut self, msg: usize) {
//         self.msg_in.push_back(msg);
        
//         for node in &self.nodes {
             
//             let msg_to_process = self.msg_in.pop_front();
        
//             match msg_to_process {
//                 Some(usize) => {
                
//                 self.msg_out.push(format!("msg {} node {}", msg, node));
//                 println!("Msg {} processed by {} for node {}", msg, self.id, node);
//                 sleep(Duration::from_millis(10)).await;
                    
//                 },
//                 None => { }
//             }       
//         }

//     }
// }

// struct Cluster {
//     id: String,
//     nodes: HashMap<String, Node>
// }

// impl Cluster {
//     async fn communicate(&mut self, msg_block: Vec<usize>) {
//         for (node_id, node) in &mut self.nodes {
//             node.say_hi().await;
            
//             for msg in &msg_block {
//                 node.process_msg(*msg).await;
//             }
//         }
//     }
// }

// #[tokio::main]
// async fn main() {

//     let mut node_a = Node { id: 1, nodes: vec!["n2".to_string()], msg_in: VecDeque::new() , msg_out: Vec::new() };
//     let mut node_b = Node { id: 2, nodes: vec!["n3".to_string()], msg_in: VecDeque::new() , msg_out: Vec::new() };
//     let mut node_c = Node { id: 3, nodes: vec!["n1".to_string()], msg_in: VecDeque::new() , msg_out: Vec::new() };
    
//     let mut cluster = Cluster { id: "cluster_a".to_owned(), nodes: HashMap::new() };
    
//     cluster.nodes.insert("n1".to_string() , node_a);
//     cluster.nodes.insert("n2".to_string() , node_b);
//     cluster.nodes.insert("n3".to_string() , node_c);

//     // for (node_id, node) in &cluster.nodes {
//     //     node.say_hi().await;
//     // };
    
//     let handle = tokio::spawn(async move {
        
//         let msg_block = (0..5).collect::<Vec<usize>>();
        
//         cluster.communicate(msg_block).await;
        
//          println!("{:?}", cluster.nodes.get("n1"));
        
//     });

//     let out = handle.await.unwrap();
    
    
    
//     println!("All done from main!");

// }

// V3

// use tokio::sync::{mpsc, Mutex};
// use tokio::time::{sleep, Duration};
// use std::collections::{HashMap, VecDeque};
// use std::sync::Arc;

// #[derive(Debug)]
// struct Node {
//     id: usize,
//     nodes: Vec<String>,
//     msg_in: Arc<Mutex<VecDeque<usize>>>,
//     msg_out: Arc<Mutex<Vec<String>>>,
// }

// impl Node {
//     async fn process_msgs(&self) {
//         loop {
//             let mut msg_in = self.msg_in.lock().await;
//             if let Some(msg) = msg_in.pop_front() {
//                 drop(msg_in); // Release the lock before processing the message

//                 for node in &self.nodes {
//                     let mut msg_out = self.msg_out.lock().await;
//                     msg_out.push(format!("Processed msg {} by node {} for node {}", msg, self.id, node));
//                     drop(msg_out); // Release the lock
//                     println!("Processed msg {} by node {} for node {}", msg, self.id, node);

//                     sleep(Duration::from_millis(10)).await; // Simulate async processing delay
//                 }
//             } else {
//                 // Sleep briefly to avoid busy-waiting
//                 sleep(Duration::from_millis(10)).await;
//             }
//         }
//     }
// }

// struct Cluster {
//     nodes: HashMap<String, Arc<Node>>,
// }

// impl Cluster {
//     async fn send_message(&self, target: &str, msg: usize) {
//         if let Some(node) = self.nodes.get(target) {
//             let mut msg_in = node.msg_in.lock().await;
//             msg_in.push_back(msg);
//             println!("Message {} sent to node {}", msg, target);
//         }
//     }

//     async fn broadcast(&self, msg_block: Vec<usize>) {
//         for (target, _) in &self.nodes {
//             for &msg in &msg_block {
//                 self.send_message(target, msg).await;
//             }
//         }
//     }
// }

// #[tokio::main]
// async fn main() {
//     let node_a = Arc::new(Node {
//         id: 1,
//         nodes: vec!["n2".to_string()],
//         msg_in: Arc::new(Mutex::new(VecDeque::new())),
//         msg_out: Arc::new(Mutex::new(Vec::new())),
//     });
//     let node_b = Arc::new(Node {
//         id: 2,
//         nodes: vec!["n3".to_string()],
//         msg_in: Arc::new(Mutex::new(VecDeque::new())),
//         msg_out: Arc::new(Mutex::new(Vec::new())),
//     });
//     let node_c = Arc::new(Node {
//         id: 3,
//         nodes: vec!["n1".to_string()],
//         msg_in: Arc::new(Mutex::new(VecDeque::new())),
//         msg_out: Arc::new(Mutex::new(Vec::new())),
//     });

//     let cluster = Cluster {
//         nodes: HashMap::from([
//             ("n1".to_string(), node_a.clone()),
//             ("n2".to_string(), node_b.clone()),
//             ("n3".to_string(), node_c.clone()),
//         ]),
//     };

//     // Start processing messages for all nodes in the background
//     for node in cluster.nodes.values() {
//         let node_clone = node.clone();
//         tokio::spawn(async move {
//             node_clone.process_msgs().await;
//         });
//     }

//     // Simulate sending messages
//     let msg_block = vec![1, 2, 3, 4, 5];
//     cluster.broadcast(msg_block).await;

//     // Allow some time for nodes to process messages
//     sleep(Duration::from_secs(2)).await;

//     // Display message logs for each node
//     for (id, node) in &cluster.nodes {
//         let msg_out = node.msg_out.lock().await;
//         println!("Node {} outgoing messages: {:?}", id, *msg_out);
//     }

//     println!("Cluster communication complete.");
// }

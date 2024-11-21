use std::io;
use std::error::Error;
use crate::session_resources::message::{self, Message};
use std::collections::{HashMap, BTreeMap};
use crate::session_resources::node::Node;
use crate::session_resources::implementation::{MessageExecutionType, Implementation, StdOut, TCP};
use crate::session_resources::message::MessageFields;
use crate::session_resources::exceptions::ClusterExceptions;

// Cluster Class
// Collection of nodes that will interact to achieve a task
#[derive(Debug, Clone)]
pub struct Cluster {
    pub cluster_id: String,
    pub nodes: BTreeMap<String, Node>,
    pub node_message_log: HashMap<usize, Message>
  }
  

  
  impl Cluster {
  
    pub fn create(cluster_reference: usize) -> Self {
      // TODO: check whether cluster with reference already exists
      Self { cluster_id: format!("cluster-{cluster_reference}"), nodes: BTreeMap::new(), node_message_log: HashMap::new() }
    }

    pub fn add_node_to_cluster(&mut self, incoming_message: Message) -> Result<(), ClusterExceptions> {

        let node_exists = self.nodes.contains_key(incoming_message.node_id().unwrap());
    
        match node_exists {
            false => {
                let new_node = Node::create(&incoming_message);
                let nodes_id_ref = *self.node_message_log.keys().max().unwrap_or(&0_usize) + 1;

                self.nodes.entry(new_node.node_id.clone()).or_insert(new_node.clone());
                self.node_message_log.insert(nodes_id_ref, incoming_message.clone());
    
                let node_check = self.nodes.contains_key(incoming_message.node_id().unwrap());

                match node_check {
                    // Existing node prints and return Self
                    true => {
                        println!("Node {} has been connected to {}", new_node.node_id, self.cluster_id);
                    },
                    false => {
                        // New node  prints and appends Self to nodes collection
                        return Err(ClusterExceptions::FailedToRetrieveNodeFromCluster { error_message: new_node.node_id } );
                    }
                }

            },
            true => {
                let retrieve_node = self.nodes.get_key_value(incoming_message.node_id().unwrap());
    
                match retrieve_node {
                    Some(retrieve_node) => {
                        println!("Node {} already exists and connected to {}", incoming_message.node_id().unwrap(), self.cluster_id);
                    },
                    None => {
                        return Err(ClusterExceptions::FailedToRetrieveNodeFromCluster { error_message: incoming_message.node_id().unwrap().to_string() } );
                    }
                    
                }
            }
        }
        Ok(())
    }

    pub fn update_node(&mut self, incoming_message: Message) -> Result<(), ClusterExceptions> {
        let node = self.nodes.get_mut(incoming_message.dest().unwrap());
    
        match node {
            // When node does exist, update and return self
            Some(node) => {
                node.message_requests.push_back(incoming_message);
            },
            // Raise Cluster exception when node does not exist
            None => {
                return Err(ClusterExceptions::NodeDoesNotExist { error_message: incoming_message.dest().unwrap().to_string()  } );
            }
        }
        Ok(())
    }
  
    pub fn add_or_update_node(&mut self, incoming_message: String) -> Result<(), ClusterExceptions> {
        let incoming_message = incoming_message.trim().to_string();

        //println!("{:?}", &incoming_message);

        let client_request = message::message_deserializer(&incoming_message)?;

        println!("{:?}", &client_request);

        match client_request {

            Message::Init {..} => {

                self.add_node_to_cluster(client_request)
                
            },
            Message::Request {..} => {          

                self.update_node(client_request)
                
            },
            _ => {
                Err(ClusterExceptions::UnkownClientRequest { error_message: client_request.body().unwrap().to_string() } )
            }
        }
  }
  
    pub fn remove(mut self, node: Node) -> Result<(), ClusterExceptions> {
  
      let node_removal = self.nodes.remove(&node.node_id);
  
      match node_removal {
        Some(node_removal) => {
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
  
    pub fn count_nodes(self) -> usize {
      self.nodes.len()
    }
  
    pub fn execute_communication(&mut self, message_execution_type: &MessageExecutionType) -> Result<(), ClusterExceptions> {
      // For each node in cluster
      
      // Need to first allocate incoming Message to Node of cluster
  
      // Once node is up and messaged queued, we can generate and execute the response
  
      // No need for loop through all Nodes here in EAGER
  
      for (node_id, node) in &mut self.nodes {
        let mut node_message_queue_ref = 0_usize;
  
        for message in node.message_requests.iter() {

            match *message {
            Message::Request {..} => {
                
                let response = message.generate_response(node_message_queue_ref);

                node.message_record.insert(node_message_queue_ref,response.clone());

                let response_string = message::message_serializer(&response)?;
                
                match *message_execution_type {
                MessageExecutionType::StdOut => {
                    let mut stdout_lock = io::stdout().lock();
                    StdOut::write_to_std_out(stdout_lock, response_string)?;
                },
                MessageExecutionType::TCP => {
                    println!("TCP has not been implemented yet")
                },
                _ => println!("Error on communication execution")
                }
                node_message_queue_ref += 1;
            },
            _ => ()
            }
        }
      }
      Ok(())
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

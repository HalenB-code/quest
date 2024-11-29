use crate::session_resources::message::{self, Message, MessageType};
use crate::session_resources::message::MessageTypeFields;
use crate::session_resources::implementation::{MessageExecutionType, StdOut};
use crate::session_resources::message::MessageFields;
use crate::session_resources::exceptions::ClusterExceptions;
use crate::session_resources::cluster::Cluster;

use std::sync::Arc;
use tokio::sync::Mutex;
use std::io;

use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use std::hash::Hash;
use std::collections::{HashMap, VecDeque};

// Node Class
// Node is the execution unit of a cluster
#[derive(Clone, Debug)]
pub struct Node {
    pub node_id: String,
    pub other_node_ids: Vec<String>,
    pub node_memory_space: HashMap<String, Vec<usize>>,
    pub message_requests: Arc<Mutex<VecDeque<Message>>>,
    pub message_responses: Arc<Mutex<VecDeque<Message>>>,
    pub message_record: Arc<Mutex<HashMap<usize, Message>>>
  }
  
  impl Node {
    // Create new instance of Node if not already in existance; existance determined by entry in nodes collection as multiple nodes may be created
    pub async fn create(client_request: &Message) -> Self {
      // Init enum used to determine whether incoming message is Init or Request
      Self { 
        node_id: (*client_request.node_id().unwrap()).clone(), 
        other_node_ids: (*client_request.node_ids().unwrap()).to_owned(),
        node_memory_space: HashMap::new(),
        message_requests: Arc::new(Mutex::new(VecDeque::new())), 
        message_responses: Arc::new(Mutex::new(VecDeque::new())), 
        message_record: Arc::new(Mutex::new(HashMap::new())) 
      }
    }
  
    // pub fn update(&mut self, client_request: Message) {
    //   if let Message::Request {..} = client_request {
    //     self.message_requests.push_back(client_request.clone());
    //   }
    // }

    pub async fn process_requests(
      &mut self,
      message_execution_type: MessageExecutionType,
      cluster: &Arc<Mutex<Cluster>>,
  ) -> Result<(), ClusterExceptions> {

      loop {
          let mut message_request = cluster.lock().await.messenger.dequeue().await;

          if let Some(message) = message_request {
              // drop(message_request); // Release lock immediately
              println!("{:?} in process_requests", &message);

              let node_message_queue_ref = {
                  let message_record = self.message_record.lock().await;
                  message_record.keys().max().unwrap_or(&0_usize) + 1
              };
  
              let mut message_response: Message;
  
              match message.body() {
                  Some(message_type) => match *message_type {
                      MessageType::Echo { .. } => {
                          message_response = Message::Response {
                              src: message.dest().unwrap().to_string(),
                              dest: message.src().unwrap().to_string(),
                              body: MessageType::EchoOk {
                                  msg_id: node_message_queue_ref,
                                  in_reply_to: message.body().unwrap().msg_id().unwrap(),
                                  echo: message.body().unwrap().echo().unwrap().to_string(),
                              },
                          };
                      }
                      MessageType::Generate { .. } => {
                          message_response = Message::Response {
                              src: message.dest().unwrap().to_string(),
                              dest: message.src().unwrap().to_string(),
                              body: MessageType::GenerateOk {
                                  msg_id: node_message_queue_ref,
                                  in_reply_to: node_message_queue_ref,
                                  id: self.generate_unique_id(),
                              },
                          };
                      }
                      MessageType::Broadcast { .. } => {
                          let incoming_broadcast_msg =
                              message.body().unwrap().broadcast_msg().unwrap();
  
                          {
                              let mut memory_space = self.node_memory_space.clone();
                              memory_space
                                  .entry("broadcast_msgs".to_string())
                                  .or_insert_with(Vec::new)
                                  .push(incoming_broadcast_msg);
                          }
  
                          for other_node in &self.other_node_ids {
                              let propagated_message = Message::Request {
                                  src: self.node_id.clone(),
                                  dest: other_node.clone(),
                                  body: MessageType::Broadcast {
                                      message: incoming_broadcast_msg.clone(),
                                  },
                              };
                              cluster.lock().await.propagate_message(propagated_message).await?;
                          }
  
                          message_response = Message::Response {
                              src: message.dest().unwrap().to_string(),
                              dest: message.src().unwrap().to_string(),
                              body: MessageType::BroadcastOk {
                                  msg_id: node_message_queue_ref,
                                  in_reply_to: node_message_queue_ref,
                              },
                          };
                      }
                      MessageType::BroadcastRead { .. } => {
                          let return_broadcast_msgs = {
                              let memory_space = self.node_memory_space.clone();
                              memory_space
                                  .get(&"broadcast_msgs".to_string())
                                  .cloned()
                                  .unwrap_or_default()
                          };
  
                          message_response = Message::Response {
                              src: message.dest().unwrap().to_string(),
                              dest: message.src().unwrap().to_string(),
                              body: MessageType::BroadcastReadOk {
                                  messages: return_broadcast_msgs,
                              },
                          };
                      }
                      MessageType::Topology { .. } => {
                          let topology_update =
                              message.body().unwrap().node_own_topology().unwrap();
  
                          if let Some(other_nodes) = topology_update.get(&self.node_id) {
                              if !other_nodes.is_empty() {
                                  self.other_node_ids = other_nodes.clone();
                              }
                          } else {
                            cluster.lock().await.propagate_message(message.clone()).await?;
                          }
  
                          message_response = Message::Response {
                              src: message.dest().unwrap().to_string(),
                              dest: message.src().unwrap().to_string(),
                              body: MessageType::TopologyOk {},
                          };
                      }
                      _ => {
                          return Err(ClusterExceptions::UnkownClientRequest {
                              error_message: "Response type not yet created".to_string(),
                          });
                      }
                  },
                  None => panic!("Request type does not exist!"),
              }
  
              {
                  let mut message_record = self.message_record.lock().await;
                  message_record.insert(node_message_queue_ref, message_response.clone());
              }
              self.message_responses.lock().await.push_back(message_response.clone());
  
              if let MessageExecutionType::StdOut = message_execution_type {
                  let serialized_response = message::message_serializer(&message_response)?;
                  let mut stdout_lock = io::stdout().lock();
                  StdOut::write_to_std_out(stdout_lock, format!("Response {}", serialized_response))?;
              }
          } else {
              tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
          }
      }
  }  
  

    pub fn generate_unique_id(&self) -> String {
      // Extract numeric part from the input string
      let seed: usize = self.node_id
      .chars()
      .filter(|c| c.is_numeric())
      .collect::<String>()
      .parse()
      .unwrap_or(0);

      // Use the numeric part as a seed for a hash-based random number generator
      let mut hasher = DefaultHasher::new();
      seed.hash(&mut hasher);
      let mut hash_value = hasher.finish() as usize;

      // Alphanumeric character set
      let charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
      let charset_len = charset.len() as usize;

      // Generate a 10-character alphanumeric ID
      let mut id = String::new();
      for _ in 0..10 {
          let index = (hash_value % charset_len) as usize;
          id.push(charset.chars().nth(index).unwrap());
          hash_value /= charset_len;
      }

      id
    }
  
  }


// trait Agentic {
//     pub fn create();
//     pub fn remove();
//     pub fn coordinator_response();
//     pub fn coordinator_inform();
//     pub fn agent_response();
//     pub fn agent_inform();
// };
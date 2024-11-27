use crate::session_resources::message::{self, Message, MessageType};
use crate::session_resources::message::MessageTypeFields;
use crate::session_resources::implementation::{MessageExecutionType, StdOut};
use crate::session_resources::message::MessageFields;
use crate::session_resources::exceptions::ClusterExceptions;

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
    pub message_requests: Arc<Mutex<VecDeque<Message>>>,
    pub message_responses: Arc<Mutex<VecDeque<Message>>>,
    pub message_record: Arc<Mutex<HashMap<usize, Message>>>
  }
  
  impl Node {
    // Create new instance of Node if not already in existance; existance determined by entry in nodes collection as multiple nodes may be created
    pub async fn create(client_request: &Message) -> Arc<Self> {
      // Init enum used to determine whether incoming message is Init or Request
      Arc::new( Self { node_id: (*client_request.node_id().unwrap()).clone(), other_node_ids: (*client_request.node_ids().unwrap()).to_owned(), message_requests: Arc::new(Mutex::new(VecDeque::new())), message_responses: Arc::new(Mutex::new(VecDeque::new())), message_record: Arc::new(Mutex::new(HashMap::new())) } )
    }
  
    // pub fn update(&mut self, client_request: Message) {
    //   if let Message::Request {..} = client_request {
    //     self.message_requests.push_back(client_request.clone());
    //   }
    // }

    pub async fn process_requests(&self, message_execution_type: MessageExecutionType) -> Result<(), ClusterExceptions> {
      loop {
          let mut message_requests = self.message_requests.lock().await;
  
          if let Some(message) = message_requests.pop_front() {
              let node_message_queue_ref = self.message_record.lock().await.keys().max().unwrap_or(&0_usize) + 1;
              let message_response: Message;
  
              // Process the message based on its type
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
                      _ => {
                          return Err(ClusterExceptions::UnkownClientRequest {
                              error_message: "Response type not yet created".to_string(),
                          });
                      }
                  },
                  _ => panic!("Request type does not exist!"),
              }
  
              // Store the response and add it to the response queue
              {
                  let mut message_record = self.message_record.lock().await;
                  message_record.insert(node_message_queue_ref, message_response.clone());
              }
              self.message_responses.lock().await.push_back(message_response.clone());
  
              // Write the response to StdOut
              if let MessageExecutionType::StdOut = message_execution_type {
                  let serialized_response = message::message_serializer(&message_response)?;
                  let mut stdout_lock = io::stdout().lock();
                  StdOut::write_to_std_out(stdout_lock, format!("Response {}", serialized_response))?;
              }
          } else {
              drop(message_requests); // Explicitly drop the lock
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
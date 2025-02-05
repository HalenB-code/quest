use crate::session_resources::message::{self, Message, MessageType};
use crate::session_resources::message::MessageTypeFields;
use crate::session_resources::implementation::{MessageExecutionType, StdOut};
use crate::session_resources::message::{MessageFields, MessageExceptions};
use crate::session_resources::exceptions::ClusterExceptions;
use crate::session_resources::cluster::Cluster;
use crate::session_resources::datastore::{DataFrame, Column};

use std::sync::Arc;
use tokio::sync::Mutex;
use std::io;

use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use std::hash::Hash;
use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;

//
// Node Class
// Node is the execution unit of a cluster
//


#[derive(Clone, Debug)]
pub struct Node {
    pub node_id: String,
    pub node_role_type: NodeRoleType,
    pub other_node_ids: Vec<String>,
    pub node_memory_space: HashMap<String, Vec<usize>>,
    pub message_requests: Arc<Mutex<VecDeque<Message>>>,
    pub message_responses: Arc<Mutex<VecDeque<Message>>>,
    pub message_record: Arc<Mutex<HashMap<usize, Message>>>,
    pub datastore: HashMap<String, DataFrame>,
}

pub trait NodeDataStoreTrait {
    fn get_dataframe(&self, name: &str) -> Option<&DataFrame>;
    fn get_dataframe_mut(&mut self, name: &str) -> Option<&mut DataFrame>;
    fn insert_dataframe(&mut self, name: String, df: DataFrame);
}

impl Node {
    pub fn get_dataframe(&self, name: &str) -> Option<&DataFrame> {
        self.datastore.get(name)
    }

    pub fn get_dataframe_mut(&mut self, name: &str) -> Option<&mut DataFrame> {
        self.datastore.get_mut(name)
    }

    pub fn insert_dataframe(&mut self, name: String, df: DataFrame) {
        self.datastore.insert(name, df);
    }

    pub fn append_to_dataframe(&mut self, name: &str, new_row: HashMap<String, Column>, append_new: bool) {
        if let Some(df) = self.get_dataframe_mut(name) {
            df.append_row(new_row, append_new);
        }
    }

    pub fn mutate_dataframe<F>(&mut self, name: &str, filter: F, mutation_func: fn(&mut Column))
    where
        F: Fn(&DataFrame) -> bool,
    {
        if let Some(df) = self.get_dataframe_mut(name) {
            if filter(df) {
                for col in df.columns.values_mut() {
                    mutation_func(col);
                }
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub enum NodeDataStoreTypes {
    DataFrameStore,
    // Other types can be added here
}

#[derive(Debug, Clone)]
pub enum NodeRoleType {
    Leader,
    Follower
}
  
  impl Node {
    // Create new instance of Node if not already in existance; existance determined by entry in nodes collection as multiple nodes may be created
    pub async fn create(client_request: &Message) -> Self {

    let node_name = (*client_request.node_id().unwrap()).clone();

      // Init enum used to determine whether incoming message is Init or Request
      Self { 
        node_id: node_name.clone(),
        // FIFS: First In, First Selected as leader
        node_role_type: if node_name == "n1".to_string() {NodeRoleType::Leader} else {NodeRoleType::Follower},
        other_node_ids: (*client_request.node_ids().unwrap()).to_owned(),
        node_memory_space: HashMap::new(),
        message_requests: Arc::new(Mutex::new(VecDeque::new())), 
        message_responses: Arc::new(Mutex::new(VecDeque::new())), 
        message_record: Arc::new(Mutex::new(HashMap::new())),
        datastore: HashMap::new(),
      }
    }


    pub async fn process_requests(
      &mut self,
      message_execution_type: MessageExecutionType,
      cluster: &Arc<Mutex<Cluster>>,
  ) -> Result<(), ClusterExceptions> {

      loop {
          let message_request = cluster.lock().await.messenger.dequeue().await;

          if let Some(message) = message_request {
              // drop(message_request); // Release lock immediately
              println!("{:?} in process_requests", &message);
              //println!("{} is active", &self.node_id);

              let node_message_queue_ref = self.node_message_log_id().await;
  
              let message_response: Message;
              let mut propagation_message_handles = Vec::new();
            
            // TODO
            // Move the message response generation logic to Message
              match message.body() {
                  Some(message_type) => match *message_type {
                      MessageType::Echo { .. } => {
                          message_response = Message::Response {
                              src: message.dest().unwrap().to_string(),
                              dest: message.src().unwrap().to_string(),
                              body: MessageType::EchoOk {
                                  msg_id: message.body().unwrap().msg_id().unwrap(),
                                  in_reply_to: message.body().unwrap().msg_id().unwrap(),
                                  echo: message.body().unwrap().echo().unwrap().to_string(),
                              },
                          };
                      },
                      MessageType::Generate { .. } => {
                          message_response = Message::Response {
                              src: message.dest().unwrap().to_string(),
                              dest: message.src().unwrap().to_string(),
                              body: MessageType::GenerateOk {
                                  msg_id: message.body().unwrap().msg_id().unwrap(),
                                  in_reply_to: message.body().unwrap().msg_id().unwrap(),
                                  id: self.generate_unique_id(),
                              },
                          };
                      },
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
                          
                          let other_nodes = self.other_node_ids.clone().into_iter().filter(|n| n != &self.node_id).collect::<Vec<String>>();
                          
                          for other_node in &other_nodes {

                              let propagated_message = Message::Request {
                                  src: self.node_id.clone(),
                                  dest: other_node.clone(),
                                  body: MessageType::Broadcast {
                                      message: incoming_broadcast_msg.clone(),
                                  },
                              };

                              let cluster_clone = Arc::clone(cluster);
                              // Spawn an async task for each propagation
                              let handle = tokio::spawn(async move {
                                  if let Err(e) = cluster_clone.lock().await.propagate_message(propagated_message).await {
                                      eprintln!("Propagation failed: {:?}", e);
                                  }
                              });
          
                              propagation_message_handles.push(handle);
                          }

                          message_response = Message::Response {
                              src: message.dest().unwrap().to_string(),
                              dest: message.src().unwrap().to_string(),
                              body: MessageType::BroadcastOk {
                                  msg_id: node_message_queue_ref,
                                  in_reply_to: node_message_queue_ref,
                              },
                          };
                      },
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
                      },
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
                      },
                      MessageType::VectorAdd { .. } => {

                        // A VectorAdd is a global counter that needs to be consistent across nodes, eventually
                        // Hence a VectorAdd request leads to 1) update on receiving node and 2) a series of follow up requests to update all other nodes so they are consistent
                        if let Some(delta) = message.body().unwrap().delta() {
                            
                            if let None = self.get_dataframe(&"df_vector".to_string()) {
                                let mut insert_data = HashMap::new();
                                insert_data.insert("Counter".to_string(), delta.as_str());

                                let df = DataFrame::new(Some(insert_data));
                                self.insert_dataframe("df_vector".to_string(), df);
                            }

                            // Now build additional VectorAdd requests, exlcuding calling node
                            let other_nodes = self.other_node_ids.clone().into_iter().filter(|n| n != &self.node_id).collect::<Vec<String>>();

                            for other_node in other_nodes {

                                let delta_request = Message::Request {
                                    src: self.node_id,
                                    dest: other_node,
                                    body: MessageType::VectorAdd { 
                                        delta: delta.clone()
                                    }
                                };

                                cluster.lock().await.propagate_message(delta_request).await?;

                            }

                            message_response = Message::Response {
                                src: message.dest().unwrap().to_string(),
                                dest: message.src().unwrap().to_string(),
                                body: MessageType::VectorAddOk {},
                            };
                        };
                      },
                      MessageType::VectorRead { .. } => {

                        let df = self.get_dataframe_mut(&"df_vector".to_string());

                        if let Some(df) = df {
                            if let Some(global_counter) = df.sum("Counter".to_string()) {
  
                            message_response = Message::Response {
                                src: message.dest().unwrap().to_string(),
                                dest: message.src().unwrap().to_string(),
                                body: MessageType::VectorReadOk { 
                                    value: global_counter.to_string()
                                },
                            };
                            }
                        }
                      },
                      MessageType::Send { .. } => {
                        // TODO
                        // Here we are explicity tying usize type to KeyValue method
                        if let Some(key) = message.body().unwrap().kv_key() {
                            
                            if let Some(value) = message.body().unwrap().kv_value() {

                                if let None = self.get_dataframe(&"df_keyvalue".to_string()) {
                                    let mut insert_data = HashMap::new();
                                    insert_data.insert(key.to_string(), value.to_string());

                                    let df = DataFrame::new(Some(insert_data));
                                    self.insert_dataframe("df_keyvalue".to_string(), df);
                                }

                                let df = self.get_dataframe_mut(&"df_keyvalue".to_string());

                                if let Some(df) = df {
                                    let mut offset_data = HashMap::new();
                                    offset_data.insert(key.clone(), value.clone());
                                    if let Some(return_offset) = df.insert_offsets(offset_data) {

                                        message_response = Message::Response {
                                            src: message.dest().unwrap().to_string(),
                                            dest: message.src().unwrap().to_string(),
                                            body: MessageType::SendOk { 
                                                offset: return_offset
                                            }
                                        };
                                    } else {
                                        return Err(ClusterExceptions::NodeFailedToCreateDataStore {
                                            error_message: self.node_id.clone(),
                                        });
                                    }
                                } else {
                                    return Err(ClusterExceptions::InvalidClusterRequest {
                                    error_message_1: message.clone(),
                                    error_message_2: self.node_id.clone(),
                                    });
                                }

                            } else {
                                return Err(ClusterExceptions::InvalidClusterRequest {
                                error_message_1: message.clone(),
                                error_message_2: self.node_id.clone(),
                                });
                            }
                        } else {
                        return Err(ClusterExceptions::InvalidClusterRequest {
                            error_message_1: message.clone(),
                            error_message_2: self.node_id.clone(),
                            });
                        }
                        
                    },
                    MessageType::Poll { .. } => {

                        if let Some(df) = self.get_dataframe_mut(&"df_keyvalue".to_string()) {

                            if let Some(msg_offsets) = message.body().unwrap().offsets() {

                                if let Ok(return_offsets) = df.get_offsets(msg_offsets.clone()) {

                                    message_response = Message::Response {
                                        src: message.dest().unwrap().to_string(),
                                        dest: message.src().unwrap().to_string(),
                                        body: MessageType::PollOk {
                                            msgs: return_offsets
                                        }
                                    };
                                }
                                else {
                                    return Err(ClusterExceptions::MessageError(MessageExceptions::PollOffsetsError));
                                }
                            }
                            else {
                                return Err(ClusterExceptions::MessageError(MessageExceptions::PollOffsetsError));
                            }
                        } else {
                            return Err(ClusterExceptions::MessageError(MessageExceptions::PollOffsetsError));
                        }
                        
                    },
                    MessageType::CommitOffsets { .. } => {

                        if let Some(df) = self.get_dataframe_mut(&"df_keyvalue".to_string()) {

                            if let Some(msg_offsets) = message.body().unwrap().offsets() {

                                if let Ok(()) = df.committ_offsets(msg_offsets.clone()) {

                                    message_response = Message::Response {
                                        src: message.dest().unwrap().to_string(),
                                        dest: message.src().unwrap().to_string(),
                                        body: MessageType::CommitOffsetsOk {
                                        }
                                    };
                                }
                                else {
                                    return Err(ClusterExceptions::MessageError(MessageExceptions::CommitOffsetsError));
                                }
                            }
                            else {
                                return Err(ClusterExceptions::MessageError(MessageExceptions::CommitOffsetsError));
                            }
                        } else {
                            return Err(ClusterExceptions::MessageError(MessageExceptions::CommitOffsetsError));
                        }

                    },
                    MessageType::ListCommitedOffsets { .. } => {

                        if let Some(df) = self.get_dataframe_mut(&"df_keyvalue".to_string()) {

                            if let Some(keys) = message.body().unwrap().keys() {

                                if let Ok(committed_offsets) = df.list_committed_offsets(keys.clone()) {

                                    message_response = Message::Response {
                                        src: message.dest().unwrap().to_string(),
                                        dest: message.src().unwrap().to_string(),
                                        body: MessageType::ListCommitedOffsetsOk { 
                                            offsets: committed_offsets
                                        }
                                    };
                                }
                                else {
                                    return Err(ClusterExceptions::MessageError(MessageExceptions::ListCommitedOffsetsError));
                                }
                            }
                            else {
                                return Err(ClusterExceptions::MessageError(MessageExceptions::ListCommitedOffsetsError));
                            }
                        } else {
                            return Err(ClusterExceptions::MessageError(MessageExceptions::ListCommitedOffsetsError));
                        }
                    },
                    MessageType::ReadFromFile { .. } => {

                        if let Some(dataframe) = self.get_kv_store::<String>().await {

                            if let Some(msg_offsets) = message.body().unwrap().offsets() {

                                if let Some(return_offsets) = kv_log_store.get_offsets(msg_offsets.clone()) {

                                    message_response = Message::Response {
                                        src: message.dest().unwrap().to_string(),
                                        dest: message.src().unwrap().to_string(),
                                        body: MessageType::PollOk {
                                            msgs: return_offsets
                                        }
                                    };
                                }
                                else {
                                    return Err(ClusterExceptions::MessageError(MessageExceptions::PollOffsetsError));
                                }
                            }
                            else {
                                return Err(ClusterExceptions::MessageError(MessageExceptions::PollOffsetsError));
                            }
                        } else {
                            return Err(ClusterExceptions::MessageError(MessageExceptions::PollOffsetsError));
                        }
                    },
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
                  let stdout_lock = io::stdout().lock();
                  StdOut::write_to_std_out(stdout_lock, format!("Response {}", serialized_response))?;
              }

              for handle in propagation_message_handles {
                if let Err(e) = handle.await {
                    eprintln!("Propagation task failed: {:?}", e);
                }
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

    pub async fn node_message_log_id(&self) -> usize {
        let message_record = self.message_record.lock().await;
        message_record.keys().max().unwrap_or(&0_usize) + 1
    }

    

  }
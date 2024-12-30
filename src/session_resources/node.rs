use crate::session_resources::message::{self, Message, MessageType};
use crate::session_resources::message::MessageTypeFields;
use crate::session_resources::implementation::{MessageExecutionType, StdOut};
use crate::session_resources::message::{MessageFields, MessageExceptions};
use crate::session_resources::exceptions::ClusterExceptions;
use crate::session_resources::cluster::Cluster;
use crate::session_resources::datastore::{NodeDataStoreTypes, NodeDataStoreTrait, NodeDataStoreObjects, ClusterDataStoreTypes, ClusterDataStoreObjects};

use std::sync::Arc;
use tokio::sync::Mutex;
use std::io;

use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use std::hash::Hash;
use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;

use super::datastore::{KeyValue, Vector};

// Node Class
// Node is the execution unit of a cluster
#[derive(Clone, Debug)]
pub struct Node {
    pub node_id: String,
    pub node_role_type: NodeRoleType,
    pub other_node_ids: Vec<String>,
    pub node_memory_space: HashMap<String, Vec<usize>>,
    pub message_requests: Arc<Mutex<VecDeque<Message>>>,
    pub message_responses: Arc<Mutex<VecDeque<Message>>>,
    pub message_record: Arc<Mutex<HashMap<usize, Message>>>,
    pub node_datastore: HashMap<NodeDataStoreTypes, Box<dyn NodeDataStoreTrait>>,
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
        node_datastore: HashMap::new(),
      }
    }

    pub async fn get_vector_store<T>(&mut self) -> Option<&Vec<T>>
    where
        T: Clone + Eq + std::hash::Hash + Send + Sync + 'static + std::fmt::Debug,
    {
        let node_datastore = self.node_datastore.get(&NodeDataStoreTypes::Vector);
        if let Some(store) = node_datastore {
            if let Some(concrete_store) = store.as_any().downcast_ref::<NodeDataStoreObjects<T>>()
            {
                // Extract NodeVector<T> and return its Arc<Mutex<Vec<T>>>
                if let NodeDataStoreObjects::Vector(vector) = concrete_store {
                    return Some(&vector.data);
                }
            }
        }
  
        None
    }

    pub async fn get_kv_store<T>(&self) -> Option<&KeyValue<T>>
    where
        T: Clone + Eq + std::hash::Hash + Send + Sync + 'static + std::fmt::Debug,
    {
        let node_datastore = &self.node_datastore.get(&NodeDataStoreTypes::KeyValue);
        if let Some(store) = node_datastore {
            if let Some(concrete_store) = store.as_any().downcast_ref::<NodeDataStoreObjects<T>>() {
                // Extract NodeVector<T> and return its Arc<Mutex<Vec<T>>>
                if let NodeDataStoreObjects::KeyValue(kv_log) = concrete_store {
                    return Some(&kv_log);
                }
            }
        }
        None
    }

    pub fn register_datastore<T>(&mut self, typ: NodeDataStoreTypes, store: NodeDataStoreObjects<T>)
    where
        T: Clone + Eq + Hash + Send + Sync + 'static + Debug,
    {
        self.node_datastore.insert(typ, Box::new(store));
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
                        cluster.lock().await.propagate_update(message.clone()).await?;
  
                        message_response = Message::Response {
                            src: message.dest().unwrap().to_string(),
                            dest: message.src().unwrap().to_string(),
                            body: MessageType::VectorAddOk {},
                        };
                      },
                      MessageType::VectorRead { .. } => {
                        let mut vector_return = String::new();
                        let cluster_lock = cluster.lock().await;

                        // TODO
                        // Method to manage the accessing of the datastore objects
                        if let Some(store) = cluster_lock.datastore.get(&ClusterDataStoreTypes::Vector) {
                            if let Some(vector_store) = store.as_any().downcast_ref::<ClusterDataStoreObjects<String>>() {
                                if let ClusterDataStoreObjects::Vector(data) = vector_store {
                                    if let Some(string) = data.dump().await {
                                        vector_return = string;
                                    }
                                }
                            }
                        }
  
                        message_response = Message::Response {
                            src: message.dest().unwrap().to_string(),
                            dest: message.src().unwrap().to_string(),
                            body: MessageType::VectorReadOk { 
                                value: vector_return
                            },
                        }; 
                      },
                      MessageType::Send { .. } => {
                        // TODO
                        // Here we are explicity tying usize type to KeyValue method
                        if let Some(key) = message.body().unwrap().kv_key() {
                            
                            if let Some(value) = message.body().unwrap().kv_value() {

                                if let None = self.get_kv_store::<String>().await {
                                    self.register_datastore(NodeDataStoreTypes::KeyValue, NodeDataStoreObjects::KeyValue(KeyValue::<String>::create()));
                                }

                                if let Some(kv_log_store) = self.get_kv_store().await {
                                    if let Some( return_offset ) = kv_log_store.clone().insert_offsets(key, value.clone()) {

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

                        if let Some(kv_log_store) = self.get_kv_store::<String>().await {

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
                    MessageType::CommitOffsets { .. } => {

                        if let Some(kv_log_store) = self.get_kv_store::<String>().await {

                            if let Some(msg_offsets) = message.body().unwrap().offsets() {

                                if let Ok(()) = kv_log_store.clone().commit_offsets(msg_offsets.clone()) {

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

                        if let Some(kv_log_store) = self.get_kv_store::<String>().await {

                            if let Some(keys) = message.body().unwrap().keys() {

                                if let Some(committed_offsets) = kv_log_store.list_commited_offsets(keys.clone()) {

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


// trait Agentic {
//     pub fn create();
//     pub fn remove();
//     pub fn coordinator_response();
//     pub fn coordinator_inform();
//     pub fn agent_response();
//     pub fn agent_inform();
// };
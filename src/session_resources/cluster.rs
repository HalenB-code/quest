use std::io;
use std::error::Error;
use crate::session_resources::message::{self, Message};
use std::collections::{HashMap, BTreeMap};
use crate::session_resources::node::Node;
use crate::session_resources::implementation::{MessageExecutionType, Implementation, StdOut, TCP};
use crate::session_resources::message::MessageFields;

// Cluster Class
// Collection of nodes that will interact to achieve a task
#[derive(Debug, Clone)]
pub struct Cluster {
    pub cluster_id: String,
    pub nodes: BTreeMap<String, Node>,
    pub node_message_log: HashMap<usize, Message>
  }
  
  pub enum ClusterExceptions {
    NodeAlreadyAttachedToCluster,
    NodeNotAttachedToCluster,
    FailedToRemoveNodeFromCluster,
    FailedToDeserializeClientRequest
  }
  
  impl Cluster {
  
    pub fn create(cluster_reference: usize) -> Self {
      // TODO: check whether cluster with reference already exists
      Self { cluster_id: format!("cluster-{cluster_reference}"), nodes: BTreeMap::new(), node_message_log: HashMap::new() }
    }
  
    pub fn add_or_update_node(&mut self, incoming_message: String) -> Result<(), Box<dyn std::error::Error>> {
        let incoming_message = incoming_message.trim().to_string();
        println!("{:?}", &incoming_message);
        let client_request = message::message_deserializer(&incoming_message)?;

        println!("{:?}", &client_request);

        match client_request {

        Message::Init {..} => {
            
            let node_exists = self.nodes.contains_key(client_request.node_id().unwrap());
    
            match node_exists {
                false => {
                    let new_node = Node::create(&client_request);
                    let nodes_id_ref = *self.node_message_log.keys().max().unwrap_or(&0_usize) + 1;

                    self.nodes.entry(new_node.node_id.clone()).or_insert(new_node.clone());
                    self.node_message_log.insert(nodes_id_ref, client_request.clone());
        
                    let node_check = self.nodes.contains_key(client_request.node_id().unwrap());

                    match node_check {
                    // Existing node prints and return Self
                    true => {
                        println!("Node {} has been connected to {}", new_node.node_id, self.cluster_id);
                    },
                    false => {
                        // New node  prints and appends Self to nodes collection
                        println!("Exception on connecting node {} to cluster {}", new_node.node_id, self.cluster_id); 
                    }
                    }
                },
                true => {
                    let retrieve_node = self.nodes.get_key_value(client_request.node_id().unwrap());
        
                    match retrieve_node {
                    Some(retrieve_node) => {
                        println!("Node {} already exists and connected to {}", client_request.node_id().unwrap(), self.cluster_id);
                    },
                    None => panic!("Node {} exists but failed to retrieve object from cluster", client_request.node_id().unwrap())
                    }
                }
            }
        },
        Message::Request {..} => {          
            
            let node = self.nodes.get_mut(client_request.dest().unwrap());
    
            match node {
            // When Node does exist, update and return self
            Some(node) => {
                node.message_requests.push_back(client_request);
            },
            None => {
                panic!("Node {} does not exist so message request cannot be executed", node.unwrap().node_id);
            }
            }
        },
        _ => {
            eprintln!("Unknown message type {:?}", client_request);
        }
        
        }
    Ok(())
  }
  
    pub fn remove(mut self, node: Node) {
  
      let node_removal = self.nodes.remove(&node.node_id);
  
      match node_removal {
        Some(node_removal) => {
          println!("{} has been disconnected from {}", node.node_id, self.cluster_id); 
        },
        None => {
          println!("Failed to disconnect {} from {}", node.node_id, self.cluster_id); 
        }
      };
    }
  
    pub fn terminate(mut self) {
      let node_count = self.nodes.is_empty();
  
      match node_count {
        true => {println!("{} does not have any nodes allocated", self.cluster_id)},
        false => {
          for key in &self.get_nodes() {
            println!("Removing {} from {}", key, self.cluster_id);
            self.nodes.remove(&key.clone());
          }
          println!("All nodes removed from {:?}", self.cluster_id);
          drop(self);
        }
      }
    }
  
    pub fn get_nodes(&self) -> Vec<String> {
      self.nodes.keys().cloned().collect()
    }
  
    pub fn count_nodes(self) -> usize {
      self.nodes.len()
    }
  
    pub fn execute_communication(&mut self, message_execution_type: &MessageExecutionType) -> Result<(), Box<dyn std::error::Error>> {
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
  
      // Iterate through message_requests
  
      // Append each request to Cluster log
  
      // Create a response for matching request (i.e. Echo => EchoOk)
  
      // Append response to Cluster log
  
      // Execute response according to the execution type
    }
  }
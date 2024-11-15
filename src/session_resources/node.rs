use super::message::{self, Message};
use std::collections::{HashMap, VecDeque};
use crate::session_resources::message::MessageFields;

// Node Class
// Node is the execution unit of a cluster
#[derive(Clone, Debug)]
pub struct Node {
    pub node_id: String,
    pub other_node_ids: Vec<String>,
    pub message_requests: VecDeque<Message>,
    pub message_responses: VecDeque<Message>,
    pub message_record: HashMap<usize, Message>
  }
  
  impl Node {
    // Create new instance of Node if not already in existance; existance determined by entry in nodes collection as multiple nodes may be created
    pub fn create(client_request: &Message) -> Self {
      // Init enum used to determine whether incoming message is Init or Request
      Self { node_id: (*client_request.node_id().unwrap()).clone(), other_node_ids: (*client_request.node_ids().unwrap()).to_owned(), message_requests: VecDeque::new(), message_responses: VecDeque::new(), message_record: HashMap::new() }
    }
  
    pub fn update(&mut self, client_request: Message) {
      if let Message::Request {..} = client_request {
        self.message_requests.push_back(client_request.clone());
      }
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
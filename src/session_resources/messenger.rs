use crate::session_resources::message::{self, Message};
use std::collections::{HashMap, VecDeque};
use crate::session_resources::exceptions::ClusterExceptions;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::session_resources::cluster::Cluster;

#[derive(Debug, Clone)]
pub struct Messenger {
    pub message_requests: Arc<Mutex<VecDeque<Message>>>,
    pub message_responses: Arc<Mutex<VecDeque<Message>>>,
    pub message_record: HashMap<usize, Message>
}

impl Messenger {

    pub fn create() -> Self {
        Self { message_requests: Arc::new(Mutex::new(VecDeque::new())), message_responses: Arc::new(Mutex::new(VecDeque::new())), message_record: HashMap::new( ) }
    }

    pub async fn categorize(&mut self, incoming_request: String) -> Result<Message, ClusterExceptions> {
        let message_request = message::message_deserializer(&incoming_request)?;
        Ok(message_request)
    }

    pub async fn request_queue(&mut self, incoming_request: Message) -> Result<(), ClusterExceptions> {

        let message_ref = self.message_record
        .keys()
        .max()
        .unwrap_or(&0_usize) + 1;

        self.message_record
        .insert(message_ref, incoming_request.clone());

        self.message_requests
        .lock()
        .await
        .push_back(incoming_request);

        Ok(())

    }

    pub async fn response_queue(&mut self, incoming_request: Message) -> Result<(), ClusterExceptions> {

        let message_ref = self.message_record
        .keys()
        .max()
        .unwrap_or(&0_usize) + 1;

        self.message_record
        .insert(message_ref, incoming_request.clone());

        self.message_responses
        .lock()
        .await
        .push_back(incoming_request);

        Ok(())

    }

    pub async fn dequeue(&mut self) -> Option<Message> {
        let mut queue = self.message_requests.lock().await;

        let next_message = queue.pop_front();
    
        if let Some(message) = &next_message {
            let message_ref = self.message_record.keys().max().unwrap_or(&0_usize) + 1;
            self.message_record.insert(message_ref, message.clone());
        }
        next_message
    }
    
}
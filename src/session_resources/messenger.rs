use crate::session_resources::message::{self, Message};
use std::collections::{HashMap, VecDeque};
use crate::session_resources::exceptions::ClusterExceptions;
use std::sync::Arc;
use tokio::sync::Mutex;

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

    pub async fn queue(&mut self, incoming_request: String) -> Result<Message, ClusterExceptions> {

        let message_request = message::message_deserializer(&incoming_request)?;

        let message_ref = self.message_record
        .keys()
        .max()
        .unwrap_or(&0_usize) + 1;

        self.message_record
        .insert(message_ref, message_request.clone());

        self.message_requests
        .lock()
        .await
        .push_back(message_request.clone());

        Ok(message_request)

    }

    pub async fn requeue(&mut self, incoming_request: Message) -> Result<Message, ClusterExceptions> {

        let message_ref = self.message_record
        .keys()
        .max()
        .unwrap_or(&0_usize) + 1;

        self.message_record
        .insert(message_ref, incoming_request.clone());

        self.message_requests
        .lock()
        .await
        .push_back(incoming_request.clone());

        Ok(incoming_request)

    }

    pub async fn dequeue(&mut self) -> Option<Message> {

        let next_message = self.message_requests
        .lock()
        .await
        .pop_front();

        match &next_message {
            Some(message) => {

                let message_ref = self.message_record
                .keys()
                .max()
                .unwrap_or(&0_usize) + 1;
        
                self.message_record
                .insert(message_ref, message.clone());

            return next_message;

            },
            None => {
                None
            }
        }
    }
}
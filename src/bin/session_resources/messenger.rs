use crate::session_resources::message::{self, Message};
use std::collections::{HashMap, VecDeque};
use crate::session_resources::exceptions::ClusterExceptions;
use std::sync::Arc;
use tokio::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub struct Messenger {
    pub message_requests: Arc<Mutex<VecDeque<Message>>>,
    pub message_responses: Arc<Mutex<VecDeque<Message>>>,
    pub message_record: Arc<Mutex<HashMap<usize, Message>>>,
    pub next_id: Arc<AtomicUsize>,
    pub response_message_channel: mpsc::Sender<String>,
}

impl Messenger {
    pub fn create(cluster_sending_channel: mpsc::Sender<String>) -> Self {
        Self { 
            message_requests: Arc::new(Mutex::new(VecDeque::new())), 
            message_responses: Arc::new(Mutex::new(VecDeque::new())), 
            message_record: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(AtomicUsize::new(1)) ,
            response_message_channel: cluster_sending_channel
        }
    }

    pub async fn categorize(&mut self, incoming_request: String) -> Result<Message, ClusterExceptions> {
        let message_request = message::message_deserializer(&incoming_request)?;
        Ok(message_request)
    }

    pub async fn request_queue(&self, incoming_request: Message)
        -> Result<(), ClusterExceptions>
    {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);

        {
            let mut record = self.message_record.lock().await;
            record.insert(id, incoming_request.clone());
        }

        self.message_requests
            .lock()
            .await
            .push_back(incoming_request);

        Ok(())
    }

    pub async fn response_queue(&self, incoming_request: Message)
        -> Result<(), ClusterExceptions>
    {
        // let id = self.next_id.fetch_add(1, Ordering::Relaxed);

        // {
        //     let mut record = self.message_record.lock().await;
        //     record.insert(id, incoming_request.clone());
        // }

        // self.message_responses
        //     .lock()
        //     .await
        //     .push_back(incoming_request);

        self.response_message_channel.send(message::message_serializer(&incoming_request).unwrap()).await.map_err(|error| ClusterExceptions::ClusterReceivingChannelSendError{error_message: "Sending response error".into()})?;

        Ok(())
    }

    pub async fn dequeue(&mut self) -> Option<Message> {
        let mut queue = self.message_requests.lock().await;

        let next_message = queue.pop_front();
    
        if let Some(message) = &next_message {
            let id = self.next_id.fetch_add(1, Ordering::Relaxed);
            self.message_record.lock().await.insert(id, message.clone());
        }
        next_message
    }
    
}
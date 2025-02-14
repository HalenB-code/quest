use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt;
use tokio::sync::mpsc;

use std::io::{Read, Write};
use std::net::TcpStream;
use tokio::sync::broadcast;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::session_resources::config::Network;
use crate::session_resources::message::{self, Message, MessageExceptions};

use super::exceptions::ClusterExceptions;
use super::message::MessageFields;

#[derive(Debug, Clone)]
pub enum ShuffleType {
    Ring
}

#[derive(Debug, Clone)]
pub struct NetworkManager {
    pub cluster_address: String,
    pub connections: HashMap<String, Arc<Mutex<TcpStream>>>,
    pub cluster_sending_channel: mpsc::Sender<String>,
    pub remote_sending_channel: Arc<Mutex<broadcast::Sender<Message>>>,
    pub remote_receiver_channel: Arc<Mutex<broadcast::Receiver<Message>>>,
    pub network_map: HashMap<String, (String, NodeType)>,
    pub shuffle_pattern: ShuffleType
}

#[derive(Debug, Clone)]
pub enum NodeType {
    Local,
    Remote
}

impl fmt::Display for NodeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeType::Local { .. } => write!(f, "Local"),
            NodeType::Remote { .. } => write!(f, "Remote"),
        }
    }
}

impl NetworkManager {
    pub fn new(network_config: &Network, cluster_sending_channel: mpsc::Sender<String>) -> Self {
        let mut layout = HashMap::new();
        let binding_address = network_config.clone().binding_address;
        let orchestrator_port = network_config.clone().orchestrator_port;
        let (sender, receiver) = broadcast::channel::<Message>(100);

        for (idx, node) in network_config.clone().layout {

            if let Some(node_name) = node.get("name") {
                match node_name.as_str() {
                    "local" => {
                        layout.insert(node_name.to_string(), (format!("{}:{}", binding_address, node.get("port").unwrap()), NodeType::Local));
                    },
                    _ => {
                        layout.insert(node_name.to_string(), (format!("{}:{}", binding_address, node.get("port").unwrap()), NodeType::Remote));
                    }
                }
            }
        }

        let mut connections = HashMap::new();
        println!("{:?}", format!("{}:{}", binding_address, orchestrator_port));
        connections.insert("cluster-orchestrator".to_string(), Arc::new(Mutex::new(TcpStream::connect(format!("{}:{}", binding_address, orchestrator_port)).unwrap())));

        Self {
            cluster_address: format!("{}:{}", binding_address, orchestrator_port),
            connections,
            cluster_sending_channel,
            remote_sending_channel: Arc::new(Mutex::new(sender)),
            remote_receiver_channel: Arc::new(Mutex::new(receiver)),
            network_map: layout,
            shuffle_pattern: ShuffleType::Ring
        }
    }

    pub fn healthcheck_response(mut stream: TcpStream) {
        let mut buffer = [0; 4]; // Expecting "ping"
        if stream.read(&mut buffer).is_ok() && buffer == *b"ping" {
            stream.write_all(b"pong").unwrap();
        }
    }

    pub fn establish_remote_connection(&mut self, node_id: String) -> Result<(), ClusterExceptions> {
        let node_address = &self.network_map.get(&node_id).unwrap().0;
        match TcpStream::connect(node_address) {
            Ok(stream) => {
                self.add_connection(node_id.clone(), stream);
                Ok(())
            }
            Err(err) => Err(ClusterExceptions::NetworkError(NetworkExceptions::TcpStreamError { 
                error_message: format!("Failed to connect to {}: {}", node_id, err) 
            }))
        }
    }

    pub async fn create_network(&mut self) -> Result<(), ClusterExceptions> {
                // TODO
        // SSH and binary file placement on remote file system
        // For now we assume this is inplace

        for (node, (address, node_type)) in self.network_map.clone() {
            match node_type {
                NodeType::Local => {
                    // This channel is local and will be read by local nodes
                    self.cluster_sending_channel.send(format!(r#"{{"type":"init","msg_id":999,"node_id":"{node}","node_ids":[]}}"#)).await;
                    
                },
                NodeType::Remote => {
                    self.establish_remote_connection(node.to_string())?;
                }
            }
        }

        Ok(())

    }

    pub async fn start_network_workers(&mut self) {
        let receiver = &self.remote_receiver_channel;
        let receiver_clone = Arc::clone(&receiver);
    
        while let Ok(message) = receiver_clone.lock().await.recv().await {
            let node_id = message.dest().unwrap().clone();
            if let Some(node_tcp_stream) = self.get_connection(&node_id) {
                tokio::spawn(async move {
                    let serialized = serialize_tcp_response(message).unwrap();
                    let node_tcp_stream_clone = Arc::clone(&node_tcp_stream);
                    if let Err(e) = tcp_write(node_tcp_stream_clone, serialized).await {
                        eprintln!("Failed to send message to {}: {:?}", node_id, e);
                    }
                });
            }
        }
       
    }

    pub fn add_connection(&mut self, id: String, stream: TcpStream) {
        self.connections.insert(id, Arc::new(Mutex::new(stream)));
    }

    pub fn get_connection(&self, id: &str) -> Option<Arc<Mutex<TcpStream>>> {
        self.connections.get(id).cloned()
    }

}


pub fn deserialize_tcp_request(incoming_stream: Vec<u8>) -> Result<Message, ClusterExceptions> {
    let buffer = Vec::with_capacity(250);
    
    let incoming_request = unsafe { String::from_utf8_unchecked(buffer.to_vec()) };
    if let Ok(message_request) = message::message_deserializer(&incoming_request) {
        return Ok(message_request);
    } else {
        return Err(ClusterExceptions::MessageError(MessageExceptions::SerializationError));
    }

}

pub fn serialize_tcp_response(response: Message) -> Result<String, ClusterExceptions> {

    if let Ok(message_response) = message::message_serializer(&response) {
        return Ok(message_response);
    } else {
        return Err(ClusterExceptions::MessageError(MessageExceptions::SerializationError))
    }
}

pub async fn tcp_write(outgoing_stream: Arc<Mutex<TcpStream>>, response_string: String) -> Result<(), ClusterExceptions> {

    let mut outgoing_stream = outgoing_stream.lock().await;
    if let Err(error) = outgoing_stream.write_all(response_string.as_bytes()) {
        return Err(ClusterExceptions::IOError(error));
    } else {
        return Ok(());
    }

}

pub fn tcp_read(stream: &mut TcpStream) -> Result<Vec<u8>, ClusterExceptions> {
    let mut bytes: Vec<u8> = Vec::with_capacity(250);

    match stream.read_to_end(&mut bytes) {
        Ok(count) => {
            return Ok(bytes)
        },
        Err(error) => {
            return Err(ClusterExceptions::NetworkError(NetworkExceptions::TcpStreamError { error_message: error.to_string() }));
        }
    }
}

pub async fn node_tcp_write(outgoing_stream: &mut TcpStream, response_string: String) -> Result<(), ClusterExceptions> {

    if let Err(error) = outgoing_stream.write_all(response_string.as_bytes()) {
        return Err(ClusterExceptions::IOError(error));
    } else {
        return Ok(());
    }

}


#[derive(Debug, Clone)]
pub enum NetworkExceptions {
    HealthcheckFail { error_message: String },
    FailedToReceiveResponse { error_message: String },
    TcpStreamError { error_message: String }
}


impl fmt::Display for NetworkExceptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NetworkExceptions::HealthcheckFail { error_message} => write!(f, "Healthcheck on node '{}' failed.", error_message),
            NetworkExceptions::FailedToReceiveResponse { error_message} => write!(f, "Remote node '{}' received request but has not responded.", error_message),
            NetworkExceptions::TcpStreamError { error_message} => write!(f, "TCP stream error '{}'.", error_message),
        }
    }
}
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt;
// use std::net::{TcpListener, TcpStream};
use std::io::{Read, Write, BufReader};
use std::sync::Arc;
use std::process::{Command, Child, Stdio};
use std::any::Any;
// use std::sync::{mpsc, Mutex};

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
use tokio::time::{timeout, Duration};

use std::{thread, time};



use crate::session_resources::config::Network;
use crate::session_resources::message::{self, Message, MessageExceptions};

use super::exceptions::ClusterExceptions;
use crate::session_resources::message::MessageFields;

#[derive(Debug, Clone)]
pub enum ShuffleType {
    Ring
}

#[derive(Debug, Clone)]
pub struct NetworkManager {
    pub cluster_address: String,
    pub connections: HashMap<String, Arc<Mutex<TcpStream>>>,
    pub cluster_sending_channel: mpsc::Sender<String>,
    pub remote_sending_channel: mpsc::Sender<Message>,
    pub network_map: HashMap<String, (String, NodeType)>,
    pub shuffle_pattern: ShuffleType,
    pub local_path: String
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
    pub fn new(network_config: &Network, cluster_sending_channel: mpsc::Sender<String>, local_path: String, establish_network: bool, network_sending_channel: mpsc::Sender<Message>) -> Self {
        let mut layout = HashMap::new();
        let binding_address = network_config.clone().binding_address;
        let orchestrator_port = network_config.clone().orchestrator_port;

        for (idx, node) in network_config.clone().layout {

            if let Some(node_name) = node.get("name") {

                // if establish_network = true, use naming convention from config.toml to determine whether node is local/remote
                if establish_network {
                    match node_name.as_str() {
                        "local" => {
                            layout.insert(node_name.to_string(), (format!("{}:{}", binding_address, node.get("port").unwrap()), NodeType::Local));
                        },
                        _ => {
                            layout.insert(node_name.to_string(), (format!("{}:{}", binding_address, node.get("port").unwrap()), NodeType::Remote));
                        }
                    }
                // If establish_network = false, then all nodes are treated as local 
                } else {
                    continue
                 }

            }
        }

        Self {
            cluster_address: format!("{}:{}", binding_address, orchestrator_port),
            connections: HashMap::new(),
            cluster_sending_channel,
            remote_sending_channel: network_sending_channel,
            network_map: layout,
            shuffle_pattern: ShuffleType::Ring,
            local_path
        }
    }

    // pub fn healthcheck_response(mut stream: TcpStream) {
    //     let mut buffer = [0; 4]; // Expecting "ping"
    //     if stream.read(&mut buffer).is_ok() && buffer == *b"ping" {
    //         stream.write_all(b"pong").unwrap();
    //     }
    // }

    pub async fn establish_remote_connection(&mut self, node_id: String) -> Result<(), ClusterExceptions> {
        let node_address = &self.network_map.get(&node_id).unwrap().0;

        match TcpStream::connect(node_address).await {
            Ok(stream) => {
                self.add_connection(node_id.clone(), stream).await;
                return Ok(());
            }
            Err(err) => { return Err(ClusterExceptions::NetworkError(NetworkExceptions::TcpStreamError { 
                error_message: format!("Failed to connect to {}: {}", node_id, err) }));
            }
        };
    }

    pub async fn create_network(&mut self) -> Result<(), ClusterExceptions> {

        // Sequence of TCP connections follows: 
        // 1_ Cluster listener at cluster_address established
        // 2_ node initiation occurs, establishing node listener and stream connection with cluster_address the reciprocal
        // 3_ with node listener up, establish stream connection cluster side and store in connections

        let mut address_node_lookup: HashMap<String, (String, Child)> = HashMap::new();
        for (node, (address, _node_type)) in self.network_map.clone() {

            if let Ok(child) = self.remote_node_initiate(&self.cluster_address, &node, &address, &self.local_path) {
                address_node_lookup.insert(address, (node, child));
            } else {
                return Err(ClusterExceptions::RemoteNodeRequestError { error_message: "Failed to initiate node client".to_string() });
            }            
        }

        if let Ok(listener) = TcpListener::bind(self.cluster_address.clone()).await {

            // Check that the number of connections is < connections in network_map; if true, continue loop
            let mut all_nodes_exist = {if &self.connections.len() == &self.network_map.len() { false } else { true } };

            while all_nodes_exist {
                let (socket, addr) = listener.accept().await?;

                if let Some((node_id, _node_client)) = address_node_lookup.get(&addr.to_string()) {

                    if let Some((_node_address, node_type)) = self.network_map.get(node_id) {

                        match node_type {
                            NodeType::Local => {
                                // This channel is local and will be read by local nodes
                                self.cluster_sending_channel.send(format!(r#"{{"type":"init","msg_id":999,"node_id":"{node_id}","node_ids":[]}}"#)).await?;
                                
                            },
                            NodeType::Remote => {
                                self.add_connection(node_id.clone(), socket).await;
                                println!("{} connected", node_id);
                            }
                        }
                    }
                }

                all_nodes_exist = if &self.connections.len() == &self.network_map.len() { false } else { true };

            };
            
        } else {
            return Err(ClusterExceptions::NetworkError(NetworkExceptions::TcpStreamError { error_message: "failed to establish tcp connection on node {}".to_string() }));
        }

        Ok(())

    }

    pub async fn add_connection(&mut self, id: String, stream: TcpStream) {
        self.connections.insert(id, Arc::new(Mutex::new(stream)));
    }

    pub async fn get_connection(&self, id: &str) -> Option<Arc<Mutex<TcpStream>>> {
        self.connections.get(id).cloned()
    }

    pub fn remote_node_initiate(&self, cluster_bind_address: &String, node_id: &String, node_bind_address: &String, local_path: &String) -> Result<Child, ClusterExceptions> {

        let node_initiate = Command::new(r"C:\rust\projects\rust-bdc\target\debug\node_client.exe")
        .arg(cluster_bind_address)
        .arg(node_id)// node_id
        .arg(node_bind_address) // node bind address
        .arg(local_path) // local path
        .stdout(Stdio::null())
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .spawn();
        
        if let Ok(child_cmnd) = node_initiate {
            return Ok(child_cmnd);
        } else {
            return Err(ClusterExceptions::RemoteNodeRequestError { error_message: "initiate exe failed".to_string() });
        }
  
    }

}

pub async fn start_network_workers(
    network_layout: HashMap<String, Arc<Mutex<TcpStream>>>, 
    mut remote_receiver_channel: tokio::sync::mpsc::Receiver<Message>
) -> Result<(), ClusterExceptions> {
    println!("start_network_workers spawned, waiting for messages...");
    
    while let Some(message) = remote_receiver_channel.recv().await {
        println!("Received message: {:?}", message); // Debugging output

        let node_id = message.dest().unwrap().clone();
        
        if let Some(node_tcp_stream) = network_layout.get(&node_id) {
            //println!("Found connection for node {}", node_id);

            let node_tcp_stream_clone = Arc::clone(&node_tcp_stream);
            let serialized = serialize_tcp_response(message).await.unwrap();
            if let Err(e) = tcp_write(node_tcp_stream_clone, serialized).await {
                eprintln!("Failed to send message to {}: {:?}", node_id, e);
            }
        } else {
            println!("No connection found for node {}", node_id);
            return Err(ClusterExceptions::NetworkError(NetworkExceptions::HealthcheckFail { error_message: "no connection found".to_string() }))
        }
    }
    println!("Receiver channel closed! Exiting start_network_workers.");

    Ok(())
}


pub async fn deserialize_tcp_request(incoming_stream: &Vec<u8>) -> Result<Message, ClusterExceptions> {
    let received_bytes = incoming_stream.iter().filter(|x| **x != 0).map(|x| *x as u8).collect::<Vec<u8>>();

    if let Ok(return_string) = String::from_utf8(received_bytes.to_vec()) {
        if let Ok(message_request) = message::message_deserializer(&return_string) {
            return Ok(message_request);
        } else {
            return Err(ClusterExceptions::MessageError(MessageExceptions::SerializationError));
        }
    } else {
        return Err(ClusterExceptions::MessageError(MessageExceptions::SerializationError));
    }

}

pub async fn serialize_tcp_response(response: Message) -> Result<String, ClusterExceptions> {

    if let Ok(message_response) = message::message_serializer(&response) {
        return Ok(message_response);
    } else {
        return Err(ClusterExceptions::MessageError(MessageExceptions::SerializationError))
    }
}

pub async fn tcp_write(outgoing_stream: Arc<tokio::sync::Mutex<TcpStream>>, response_string: String) -> Result<(), ClusterExceptions> {

    let mut outgoing_stream = outgoing_stream.lock().await;
    if let Err(error) = outgoing_stream.write_all(response_string.as_bytes()).await {
        return Err(ClusterExceptions::IOError(error));
    } else {
        return Ok(());
    }

}

pub async fn tcp_read(stream: &mut Arc<tokio::sync::Mutex<tokio::net::TcpStream>>, max_timeout_seconds: usize) -> Result<Vec<u8>, ClusterExceptions> {
    let mut remote_stream_lock = stream.lock().await;
    let (reader, _) = remote_stream_lock.split();
    let mut buf_reader = tokio::io::BufReader::new(reader);
    let mut buffer = vec![0; 1024];

    match timeout(Duration::from_secs(max_timeout_seconds as u64), buf_reader.read(&mut buffer)).await {
        Ok(_count) => {
            return Ok(buffer)
        },
        Err(error) => {
            return Err(ClusterExceptions::NetworkError(NetworkExceptions::TcpStreamError { error_message: error.to_string() }));
        }
    }
}

pub async fn node_tcp_write(outgoing_stream: &mut tokio::net::TcpStream, response_string: String) -> Result<(), ClusterExceptions> {

    if let Err(error) = outgoing_stream.write_all(response_string.as_bytes()).await {
        return Err(ClusterExceptions::IOError(error));
    } else {
        return Ok(());
    }

}

pub async fn node_tcp_read(incoming_stream: &mut tokio::net::TcpStream) -> Result<Vec<u8>, ClusterExceptions> {

    let mut bytes: Vec<u8> = Vec::with_capacity(250);

    match incoming_stream.read_to_end(&mut bytes).await {
        Ok(_count) => {
            return Ok(bytes)
        },
        Err(error) => {
            return Err(ClusterExceptions::NetworkError(NetworkExceptions::TcpStreamError { error_message: error.to_string() }));
        }
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
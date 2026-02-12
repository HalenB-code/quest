use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt;
// use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::process::{Command, Child, Stdio};

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::{timeout, Duration};

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
    pub cluster_sending_channel: mpsc::Sender<String>,
    pub executable_connections: HashMap<String, Arc<Child>>,
    pub socket_connections: HashMap<String, Arc<TcpStream>>,
    // pub network_request_readers: HashMap<String, Arc<Mutex<mpsc::Receiver<Vec<Message>>>>>,
    pub network_request_writers: HashMap<String, mpsc::Sender<Message>>,
    // pub network_response_senders: HashMap<String, mpsc::Sender<Message>>,
    pub network_response_readers: HashMap<String, Arc<Mutex<mpsc::Receiver<Vec<Message>>>>>,
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
    pub fn new(network_config: &Network, cluster_sending_channel: mpsc::Sender<String>, local_path: String, establish_network: bool) -> Self {
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
            cluster_sending_channel,
            executable_connections: HashMap::new(),
            socket_connections: HashMap::new(),
            // network_request_readers: HashMap::new(),
            network_request_writers: HashMap::new(),
            // network_response_senders HashMap::new(),
            network_response_readers: HashMap::new(),
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
            let mut all_nodes_exist = {if &self.executable_connections.len() == &self.network_map.len() { false } else { true } };

            while all_nodes_exist {
                let (mut socket, addr) = listener.accept().await?;
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                if let Some((node_id, _node_client)) = address_node_lookup.get(&addr.to_string()) {

                    if let Some((_node_address, node_type)) = self.network_map.get(node_id) {

                        match node_type {
                            NodeType::Local => {
                                // This channel is local and will be read by local nodes
                                if let Err(error) = self.cluster_sending_channel.send(format!(r#"{{"type":"init","msg_id":999,"node_id":"{node_id}","node_ids":[]}}"#)).await {
                                    println!("Error sending remote connect init");
                                }
                            },
                            NodeType::Remote => {
                                //socket.set_nodelay(true)?;
                                //socket.set_linger(Some(Duration::from_secs(60)))?;

                                // Each node will get it's own writer task
                                // This writer task will create a new receiver and sender channel, retaining the receiver on which to listen for new requests
                                // returning the sender to be stored in network_writers

                                // Here a new reader task is created for each node
                                // The reader will listen for new incoming messages on the TCPStream and send them to the sending half of the channel
                                // This receiver will then be utilised to receive new responses from nodes within the cluster context

                                let (network_response_sender, network_response_receiver) = mpsc::channel::<Vec<Message>>(100);
                                let (network_request_sender, network_request_receiver) = mpsc::channel::<Message>(100);
                                self.network_response_readers.insert(node_id.clone(), Arc::new(Mutex::new(network_response_receiver)));
                                self.network_request_writers.insert(node_id.clone(), network_request_sender);
                                tokio::spawn(handle_node_connection(socket, network_request_receiver, network_response_sender, node_id.clone()));

                                if let Err(error) = self.cluster_sending_channel.send(format!(r#"{{"src":"cluster-orchestrator","dest":"{node_id}","body":{{"type":"remote_connect"}}}}"#)).await {
                                    println!("Error sending remote connect init");
                                }

                                // self.add_connection(node_id.clone(), socket).await;
                                println!("{} connected", node_id);
                            }
                        }
                    }
                }

                all_nodes_exist = self.network_response_readers.len() < self.network_map.len();

            };
            
        } else {
            return Err(ClusterExceptions::NetworkError(NetworkExceptions::TcpStreamError { error_message: "failed to establish tcp connection on node".to_string() }));
        }

        // {
        //     self.test_remote_connections().await?;
        // }
        

        Ok(())

    }

    pub async fn add_connection(&mut self, id: String, stream: TcpStream) {
        self.socket_connections.insert(id, Arc::new(stream));
    }

    pub async fn get_connection(&self, id: &str) -> Option<Arc<TcpStream>> {
        self.socket_connections.get(id).cloned()
    }

    pub fn remote_node_initiate(&self, cluster_bind_address: &String, node_id: &String, node_bind_address: &String, local_path: &String) -> Result<Child, ClusterExceptions> {

        let node_initiate = Command::new(r"C:\rust\projects\rust-bdc\target\debug\node_client.exe")
        .arg(cluster_bind_address)
        .arg(node_id)// node_id
        .arg(node_bind_address) // node bind address
        .arg(local_path) // local path
        //.stdout(Stdio::null())
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

pub async fn start_network_workers(network_layout: HashMap<String, Arc<Mutex<TcpStream>>>, mut remote_receiver_channel: tokio::sync::mpsc::Receiver<Message>) -> Result<(), ClusterExceptions> {
    println!("start_network_workers spawned, waiting for messages...");
    
    while let Some(message) = remote_receiver_channel.recv().await {
        // println!("Received message: {:?}", message); // Debugging output

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


pub async fn handle_node_connection(mut stream: TcpStream, mut recv_requests: mpsc::Receiver<Message>, send_responses: mpsc::Sender<Vec<Message>>, node_id: String) {
    let mut buffer = vec![0; 1024];

    loop {
        tokio::select! {
            // Handle incoming requests from the cluster
            Some(message) = async {
                let msg = recv_requests.recv().await;
                msg
            } => {
                if let Some(message_target) = message.dest() {

                    if message_target == &node_id {
                        println!("Writing request to node {}", node_id);
                        let serialized = serialize_tcp_response(message).await.unwrap();
                        if let Err(e) = stream.write(&serialized.as_bytes()).await {
                            println!("Failed to send message to {}: {:?}", node_id, e);
                            continue;
                        }
                    } else {
                        println!("Message target not matching node {} {:?}", message_target, node_id);
                    }
                } else {
                    println!("Message must have valid destination {:?}", message);
                    continue;
                }
            },

            // Handle incoming responses from the remote node
            result = stream.read(&mut buffer) => {
                match result {
                    Ok(bytes) if bytes > 0 => {
                        let received_data = buffer[..bytes].to_vec();
                        println!("node handler bytes in {:?}", received_data);
                        buffer.resize(1024, 0);
                        

                        if let Ok(response) = parse_vector_bytes(&received_data).await {
                            // Send the response to the cluster
                            if let Err(e) = send_responses.send(response).await {
                                eprintln!("Failed to send response from {}: {:?}", node_id, e);
                            } else {
                                println!("Remote response sent to node {}", node_id);
                            }
                        }
                    },
                    Ok(bytes) => {
                        eprintln!("Connection closed for node {} {}", node_id, bytes);
                        break;
                    }
                    Err(e) => {
                        eprintln!("Read error from node {}: {:?}", node_id, e);
                        break;
                    }
                }
            }
        }
    }
}

pub async fn deserialize_tcp_request(incoming_stream: &Vec<u8>) -> Result<Message, ClusterExceptions> {

    match String::from_utf8(incoming_stream.clone()) {
        Ok(return_string) => {

            match message::message_deserializer(&return_string) {
                Ok(message_request) => {
                    return Ok(message_request);
                },
                Err(error) => {
                    println!("Error converting to Message {}", error);
                    return Err(ClusterExceptions::MessageError(MessageExceptions::SerializationError));
                }
            };
        },
        Err(error) => {
            println!("Error converting from utf8 bytes {}", error);
            return Err(ClusterExceptions::MessageError(MessageExceptions::SerializationError));
        }
    }

}

pub async fn serialize_tcp_response(response: Message) -> Result<String, ClusterExceptions> {

    if let Ok(message_response) = message::message_serializer(&response) {
        return Ok(message_response);
    } else {
        return Err(ClusterExceptions::MessageError(MessageExceptions::SerializationError))
    }
}

pub async fn parse_vector_bytes(bytes: &[u8]) -> Result<Vec<Message>, ClusterExceptions> {
    let mut strings_buffer: Vec<Message> = Vec::with_capacity(100);

    // Line end "\n" = 10
    for string in bytes.rsplit(|x| *x == 10) {
        println!("String {:?}", string);

        match string.is_empty() {
            true => {
            },
            false => {
                // &string[..string.len()-1] to remove "\n" as last element in rsplitted byte sequence
                match deserialize_tcp_request(&string.to_vec()).await {
                    Ok(message) => {
                        strings_buffer.push(message);
                    },
                    Err(error) => {
                        println!("Error converting byte sequence to Message {}", error);
                        return Err(ClusterExceptions::NetworkError(NetworkExceptions::TcpStreamError { error_message: "Error converting byte sequence to Message".to_string() }))
                    }
                };
            }
        }
    }

    Ok(strings_buffer)
}

pub async fn tcp_write(outgoing_stream: Arc<tokio::sync::Mutex<TcpStream>>, response_string: String) -> Result<(), ClusterExceptions> {

    let mut outgoing_stream = outgoing_stream.lock().await;
    if let Err(error) = outgoing_stream.write(response_string.as_bytes()).await {
        return Err(ClusterExceptions::IOError(error));
    } else {
        outgoing_stream.flush();
        return Ok(());
    }

}

pub async fn tcp_read(stream: &mut Arc<tokio::sync::Mutex<tokio::net::TcpStream>>, max_timeout_seconds: usize) -> Result<Vec<u8>, ClusterExceptions> {
    let mut remote_stream_lock = stream.lock().await;
    let (reader, _) = remote_stream_lock.split();
    let mut buf_reader = tokio::io::BufReader::new(reader);
    let mut buffer = vec![0; 1024];

    match timeout(Duration::from_secs(max_timeout_seconds as u64), buf_reader.read_exact(&mut buffer)).await {
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
        outgoing_stream.flush();
        return Ok(());
    }

}

pub async fn node_tcp_read(incoming_stream: &mut tokio::net::TcpStream) -> Result<Vec<u8>, ClusterExceptions> {
    let mut buf_reader = tokio::io::BufReader::new(incoming_stream);

    let mut buffer: Vec<u8> = Vec::with_capacity(1024);

    match buf_reader.read_exact(&mut buffer).await {
        Ok(_count) => {
            return Ok(buffer)
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

// pub async fn test_remote_connections(&mut self) -> Result<(), ClusterExceptions> {
//     for (node, connection) in &self.connections {
//         for i in 0..1 {
//             let mut remote_connection = connection.lock().await;

//             //println!("Linger {:?} No delay {:?} Read {:?} Read {:?}", remote_connection.linger(), remote_connection.nodelay(), remote_connection.ready(Interest::READABLE).await, remote_connection.ready(Interest::WRITABLE).await);

//             if let Ok(return_bytes) = node_tcp_read(&mut remote_connection, 2).await {
//                 match deserialize_tcp_request(&return_bytes).await {
//                     Ok(message) => {
//                         println!("In {:?}", message);
//                     },
//                     Err(_error) => {
//                         if i == 1 {
//                             println!("Failed to confirm connection for {}", node);
//                             return Err(ClusterExceptions::NetworkError(NetworkExceptions::FailedToReceiveResponse { error_message: "RemoteConnectOk not received".to_string() }))
//                         }
//                     }
//                 }
//             } 
//         }
//     }

//     Ok(())

// }
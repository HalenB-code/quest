mod session_resources;
use crate::session_resources::message::{Message, MessageType};
use crate::session_resources::implementation::MessageExecutionType;
use crate::session_resources::node::Node;
use crate::session_resources::network;
use crate::session_resources::message;

use std::env;
use std::net::TcpStream;
use tokio::net::TcpListener;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;

// TODO
// Binary for node-only functionality that will be executed on remote machines
// which need to listen for incoming requests and processes accordingly

// -- exe --cluster_address --node_name --bind_address --local_path

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();
    let cluster_address = &args[1];
    let node_id = &args[2];
    let bind_address = &args[3];
    let local_path = &args[4];

    // Errors can't be returned as this executable will communicate through TCP
    // If there is an non-TCP error, it should be communicated back to cluster
    // TCP issues will need to be handled from the cluster side
    
    let message_execution_type = MessageExecutionType::StdOut;

    // TODO
    // Capture errors in remote node log
    // For now, print to std out

    let cluster_inbound_stream = TcpListener::bind(cluster_address).await?;
    let cluster_outbound_stream = TcpStream::connect(cluster_address);

    // Generate RemoteConnectOk response once cluster_stream connected
    let message_response = Message::Response {
        src: node_id.to_string(),
        dest: "cluster-orchestrator".to_string(),
        body: MessageType::RemoteConnectOk {
        }
    };
    
    let init_message = message::message_deserializer(&format!(r#"{{"type":"init","msg_id":0,"node_id":"{node_id}","node_ids":[]}}"#))?;
    let node: Node = Node::create(&init_message, local_path).await;

    if let Ok(mut cluster_stream) = cluster_outbound_stream {
        if let Ok(message_response_serialized) = network::serialize_tcp_response(message_response) {
            match network::node_tcp_write(&mut cluster_stream, message_response_serialized).await {
                Ok(()) => {
                    println!("{} serving on {}", node_id, bind_address);
                },
                Err(error) => {
                    eprintln!("{:?}", error);
                }
            }
        }
    }


    // Node needs to be registered with cluster before it can process incoming requests
    loop {
        match cluster_inbound_stream.accept().await {
            Ok((socket, _)) => {
                let mut node = node.clone();
                let message_execution_type = message_execution_type.clone();
                let local_path = local_path.clone();
                tokio::spawn(async move {
                    let (reader, mut writer) = tokio::io::split(socket);
                    let mut reader = tokio::io::BufReader::new(reader);
                    let mut buffer = String::new();

                    match reader.read_line(&mut buffer).await {
                        Ok(_) => {
                            if let Ok(request) = network::deserialize_tcp_request(buffer.into_bytes()) {
                                match request {
                                    Message::Init { .. } => {
                                        node = Node::create(&request, &local_path).await;
                                    },
                                    _ => {
                                        if let Ok(response_messages) = node.execute(&message_execution_type, request).await {
                                            if response_messages.len() > 0 {
                                                for response_message in response_messages {
                                                    if let Ok(response_string) = network::serialize_tcp_response(response_message) {
                                                        if let Err(e) = writer.write_all(response_string.as_bytes()).await {
                                                            eprintln!("Failed to write to stream: {}", e);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        Err(e) => eprintln!("Failed to read from stream: {}", e),
                    }
                });
            },
            Err(e) => eprintln!("Failed to accept connection: {}", e),
        }
    }
    
    Ok(())
}



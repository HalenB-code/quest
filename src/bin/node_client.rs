mod session_resources;
use crate::session_resources::message::{Message, MessageType};
use crate::session_resources::implementation::MessageExecutionType;
use crate::session_resources::node::Node;
use crate::session_resources::network::{self, node_tcp_read};
use crate::session_resources::message;

use tokio::net::TcpSocket;
use std::env;
use std::sync::Arc;
use std::net::TcpStream;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::sync::Mutex;
use std::io::IoSlice;

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
    let local_path = args[4].clone();

    // Errors can't be returned as this executable will communicate through TCP
    // If there is an non-TCP error, it should be communicated back to cluster
    // TCP issues will need to be handled from the cluster side
    
    let message_execution_type = MessageExecutionType::StdOut;

    // TODO
    // Capture errors in remote node log
    // For now, print to std out
    let local_socket = TcpSocket::new_v4()?;
    local_socket.bind(bind_address.parse().expect("Valid IPV4 address"))?;

    let cluster_tcp_connection = local_socket.connect(cluster_address.parse().expect("Valid IPV4 address")).await;
    
    // Generate RemoteConnectOk response once cluster_stream connected
    let message_response = Message::Response {
        src: node_id.to_string(),
        dest: "cluster-orchestrator".to_string(),
        body: MessageType::RemoteConnectOk {
        }
    };
    
    let init_message = message::message_deserializer(&format!(r#"{{"type":"init","msg_id":0,"node_id":"{node_id}","node_ids":[]}}"#))?;
    let node: Node = Node::create(&init_message, &local_path).await;
    
    if let Ok(mut cluster_stream) = cluster_tcp_connection {
        cluster_stream.set_nodelay(true)?;
        
        let mut node = node.clone();
        let message_execution_type = message_execution_type.clone();
        let local_file_path = local_path.clone();
    
        let (reader, mut writer) = cluster_stream.split();
        let mut buf_reader = tokio::io::BufReader::new(reader);
        let mut buffer = vec![0; 1024];
    
        loop {
            match buf_reader.read(&mut buffer).await {
                Ok(bytes) if bytes > 0 => {
                    println!("Received {} bytes from cluster", bytes);
    
                    let received_data = buffer[..bytes].to_vec(); // 🔥 FIX: Only take the received bytes
                    buffer.resize(1024, 0); // 🔥 FIX: Reset buffer AFTER copying data
                    
                    if let Ok(request) = network::deserialize_tcp_request(&received_data).await {
                        match request {
                            Message::Init { .. } => {
                                node = Node::create(&request, &local_file_path).await;
                            },
                            _ => {
                                if let Ok(response_messages) = node.execute(&message_execution_type, request.clone()).await {
                                    println!("{} response vec {:?}", node_id, response_messages);

                                    let _ = node.insert_log_message(request.clone()).await;
                                    let mut output_buffer: Vec<IoSlice> = Vec::with_capacity(100);

                                    for response_message in response_messages {
                                        
                                        if let Ok(mut response_string) = network::serialize_tcp_response(response_message).await {
                                            response_string.push('\n');
                                            output_buffer.push(IoSlice::new(Box::leak(response_string.clone().into_boxed_str()).as_bytes()));
                                        }
                                    }

                                    // Small delay on first message received
                                    if node.message_record.lock().await.len() == 0 {
                                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                                    } 

                                    if let Err(e) = writer.write_vectored(&output_buffer[..]).await {
                                        eprintln!("Failed to write to stream: {}", e);
                                        continue;
                                    }                                            
                                    
                                    writer.flush().await?;
                                    tokio::task::yield_now().await;
                                    println!("Response posted to TCPStream");
                                }
                            }
                        }
                    }
                },
                Ok(_) => {
                    println!("Connection closed by cluster {}", node_id);
                    break;
                },
                Err(error) => {
                    println!("Failed to read from stream: {}", error);
                    break;
                }
            };
        }
    }
    
    println!("Main done!");
    Ok(())
    
}


        // if let Ok(message_response_serialized) = network::serialize_tcp_response(message_response).await {
        //     match network::node_tcp_write(&mut cluster_stream, message_response_serialized).await {
        //         Ok(()) => {
        //             println!("Hi from {}", node_id);
        //         },
        //         Err(error) => {
        //             eprintln!("{:?}", error);
        //         }
        //     }
        // } else {
        //     eprintln!("Failed to serialize response");
        // };

        // loop {
        //     println!("Yo");
        //     let mut buffer = vec![0; 1024];
        //     let (mut reader, mut writer) = cluster_stream.split();
        //     let mut buf_reader = tokio::io::BufReader::new(reader);
        //     let x = buf_reader.read(&mut buffer).await?;
        //     // let x = buf_reader.read_line(&mut buffer).await?;
        //     println!("Bytes {:?}", buffer);
        //     buffer.clear();
        //     //tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        // }
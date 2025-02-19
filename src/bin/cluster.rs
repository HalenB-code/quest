mod session_resources;
use std::io;
use crate::session_resources::implementation::{MessageExecutionType, Implementation};
use crate::session_resources::cluster::Cluster;
use crate::session_resources::session::Session;
use crate::session_resources::message::Message;
use crate::session_resources::network;
use crate::session_resources::message;
use std::any::Any;

use tokio::sync::mpsc;
use std::env;

#[tokio::main]
async fn main() {
  let args: Vec<String> = env::args().collect();

  let source_path = args[1].clone();
  let establish_network = args[2].clone().parse::<bool>().unwrap();

  let implementation: Implementation = Implementation::EAGER;
  let message_execution_target = MessageExecutionType::StdOut;
  let (tx, rx) = mpsc::channel::<String>(100);
  let (network_sender, network_receiver) = mpsc::channel::<Message>(100);

  let mut cluster: Cluster = Cluster::create(1, tx.clone(), rx, message_execution_target, source_path, establish_network, network_sender.clone());

  if establish_network {
      match cluster.network_manager.create_network().await {
          Ok(()) => {
            println!("Network up");
          }
          Err(error) => {
              println!("Error erecting network {:?}", error);
          }
      }
  }

  let network_connections = cluster.network_manager.connections.clone();
  tokio::spawn(network::start_network_workers(network_connections, network_receiver));

  // TODO
  // Establish network here
 
  let mut session = Session::new(cluster, implementation, tx.clone());
  
  tokio::spawn(async move {
    session.session_execution().await;
  });
  
  // This loops accepts client requests
  loop {
    let mut std_input = String::new();

    match io::stdin().read_line(&mut std_input) {
      Ok(_line) => {
        let request = std_input.trim();

        // If next STDIN message is exit, break from loop and return from session
        match request {
          "exit" => {
            break;
          },
          _ => {
            tx.send(request.to_string()).await;
          }
        }
      },
      Err(error) => eprintln!("Error reading from STDIN {error}"),
    }
  }

  println!("Main end!");

}
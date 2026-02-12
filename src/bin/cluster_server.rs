mod session_resources;
use std::io;
use crate::session_resources::implementation::{MessageExecutionType};
use crate::session_resources::cluster::Cluster;
use crate::session_resources::session::Session;

use tokio::sync::mpsc;
use std::env;

#[tokio::main]
async fn main() {
  let args: Vec<String> = env::args().collect();

  let source_path = args[1].clone();
  let establish_network = args[2].clone().parse::<bool>().unwrap();

  let message_execution_target = MessageExecutionType::StdOut;
  let (external_tx, external_rx) = mpsc::channel::<String>(100);

  let mut cluster: Cluster = Cluster::create(1, external_tx.clone(), external_rx, message_execution_target, source_path, establish_network).await;

  if establish_network {
      match cluster.network_manager.create_network().await {
          Ok(()) => {
            println!("Network up");
            // IMPORTANT: collect keys first to avoid borrow issues
            let node_ids: Vec<String> = cluster
                .network_manager
                .network_response_readers
                .keys()
                .cloned()
                .collect();

            for node_id in node_ids {
                println!("Starting response listener for {}", node_id);
                cluster.start_response_listener(node_id).await;
            }
          }
          Err(error) => {
              println!("Error erecting network {:?}", error);
          }
      }
  }

  // TODO
  // Establish network here
 
  let mut session = Session::new(cluster, external_tx.clone());
  
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
            external_tx.send(request.to_string()).await;
          }
        }
      },
      Err(error) => eprintln!("Error reading from STDIN {error}"),
    }
  }

  println!("Main end!");

}
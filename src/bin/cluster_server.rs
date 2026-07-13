mod session_resources;
use std::io;

use crate::session_resources::implementation::{MessageExecutionType};
use crate::session_resources::cluster::Cluster;
use crate::session_resources::network::NetworkManager;
use crate::session_resources::session::Session;
use crate::session_resources::message::Message;
use crate::session_resources::config::ClusterConfig;
use crate::session_resources::messenger::Messenger;

use tokio::sync::mpsc;
use std::env;

#[tokio::main]
async fn main() {
  let args: Vec<String> = env::args().collect();

  let source_path = args[1].clone();
  let establish_network = args[2].clone().parse::<bool>().unwrap();

  let cluster_config = ClusterConfig::read_config(source_path);

  let message_execution_target = MessageExecutionType::StdOut;
  let (external_tx, external_rx) = mpsc::channel::<String>(100);
  let (node_event_tx, node_event_rx) = mpsc::channel::<Message>(100);

  // Assemble network manager
  let network_layout = &cluster_config.network;
  let network_manager = NetworkManager::new(network_layout, external_tx.clone(), cluster_config.working_directory.local_path.clone(), establish_network);
  let other_nodes = network_manager.network_map.clone().into_keys().collect::<Vec<String>>();

  // Assemble messenger
  let messenger = Messenger::create(external_tx);
  let mut cluster: Cluster = Cluster::create(1, &cluster_config, network_manager, messenger, node_event_tx, message_execution_target, establish_network).await;

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

  // Establish CLI sender and receiver channels
  let (cli_tx, cli_rx) = mpsc::channel::<String>(100);
 
  let mut session = Session::new(cluster, cli_rx, external_rx, node_event_rx);
  
  tokio::spawn(async move {
    session.session_execution().await;
  });
  

  // Startup node init commands
  let node_init_message: &String = &format!(r#"-message {{"type":"init","msg_id":0,"node_id":"node-master","node_ids":{:?}}}"#, other_nodes);
  if let Err(error) = cli_tx.send(node_init_message.to_string()).await {
      println!("Error sending node init message: {:?}", error);
  }

  // This loops accepts client requests
  loop {
    let mut std_input = String::new();

    match io::stdin().read_line(&mut std_input) {
      Ok(_line) => {
        let request = std_input.trim();

        // If next STDIN message is exit, break from loop and return from session
        match request {
          "exit" => {
            println!("Exiting session");
            break;
          },
          _ => {
            cli_tx.send(request.to_string()).await.unwrap();
          }
        }
      }
      Err(error) => {
        println!("Error mapping request: {:?}", error);   
      }
    }

  }

}
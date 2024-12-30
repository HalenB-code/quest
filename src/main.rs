mod session_resources;
use std::io;
use crate::session_resources::implementation::{MessageExecutionType, Implementation};
use crate::session_resources::cluster::Cluster;
use crate::session_resources::session::Session;
use tokio::sync::mpsc;
use std::env;

#[tokio::main]
async fn main() {
  let args: Vec<String> = env::args().collect();

  let source_path = args[1].clone();

  let implementation: Implementation = Implementation::EAGER;
  let message_execution_target = MessageExecutionType::StdOut;
  let (tx, rx) = mpsc::channel::<String>(100);

  let (semantic_tx, semantic_rx) = mpsc::channel::<String>(100);

  let cluster: Cluster = Cluster::create(1, rx, message_execution_target, source_path);
 
  let mut session = Session::new(cluster, implementation, semantic_rx, tx.clone());
  
  tokio::spawn(async move {
    session.session_execution().await;
  });
  
  // This loops accepts client requests
  loop {
    let mut std_input = String::new();

    match io::stdin().read_line(&mut std_input) {
      Ok(_line) => {
        
        let (request, _other_param) = std_input.as_str().split_once(" ").unwrap();

        // If next STDIN message is exit, break from loop and return from session
        match request {
          "exit" => {
            break;
          },
          "read-file" => {
            semantic_tx.send(std_input);
          },
          _ => {
            tx.send(std_input).await;
          }
        }

      },
      Err(error) => eprintln!("Error reading from STDIN {error}"),
    }
  }

  println!("Main end!");

}


// TODO

// Statistics
// Logging from cluster and nodes
// Async await execution; arc::mutex will be required on StdOut when writing from each of the nodes
// Threading
// DAG for lazy execution model
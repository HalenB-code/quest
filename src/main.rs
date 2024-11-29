mod session_resources;
use std::io;
use crate::session_resources::implementation::{MessageExecutionType, Implementation};
use crate::session_resources::cluster::Cluster;
use crate::session_resources::session::Session;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() {

  let implementation: Implementation = Implementation::EAGER;
  let message_execution_target = MessageExecutionType::StdOut;
  let (tx, rx) = mpsc::channel::<String>(100);

  let cluster: Cluster = Cluster::create(1, rx);
 
  let mut session = Session::new(cluster, implementation, message_execution_target);
  
  tokio::spawn(async move {
    session.session_execution().await;
  });
  
  // This loops accepts client requests
  loop {
    let mut std_input = String::new();

    match io::stdin().read_line(&mut std_input) {
      Ok(_line) => {
        
        // If next STDIN message is exit, break from loop and return from session
        if std_input.trim().to_lowercase() == "exit" {
          break;
        }

        // Otherwise, deserialize message into one of expected types and continue loop
        else {        
          tx.send(std_input).await;
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
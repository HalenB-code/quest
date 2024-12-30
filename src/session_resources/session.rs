use crate::session_resources::cluster::Cluster;
use crate::session_resources::implementation::Implementation;
use crate::session_resources::exceptions::ClusterExceptions;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

// Session Class
// The session is created to manage the overall execution of client requests
// The execution engine is the cluster with the nodes that are created and connected to it
// The execution model -- eager or lazy -- is determined in the configs and dictates how incoming requests are executed
// Incoming requests are captured through STDIN and passed to the session, which will use the execution type constant to implement the execution model

#[derive(Debug)]
pub struct Session {
  pub session_id: String,
  pub cluster: Cluster,
  pub implementation_type: Implementation,
  pub semantic_receiving_channel: Arc<Mutex<mpsc::Receiver<String>>>,
  pub sending_channel: mpsc::Sender<String>,
}

// Need to add eager/lazy execution at the session level to enable streaming and batching
// Based on the selection a keyword would be sent to cluster to determine whether incoming requests
// should be implemented once received or as part of DAG
impl Session {
  // Add create method to tie session to cluster
  pub fn new(cluster: Cluster, implementation_type: Implementation, semantic_reciver: mpsc::Receiver<String>, sender: mpsc::Sender<String>) -> Session {
    return Self {
      session_id: "999".to_string(), 
      cluster: cluster.clone(), 
      implementation_type: implementation_type,
      semantic_receiving_channel: Arc::new(Mutex::new(semantic_reciver)),
      sending_channel: sender
    }
  }
  // Session execution used to handle incoming client request and execute, either lazily or eagerly
  // Might need to add Cluster object to session_context as cluster is the container for the session that all nodes are connected to and all messages will emanate to/from
  pub async fn session_execution(&mut self) {
    
    match self.implementation_type {

      Implementation::EAGER => {

        let receiver_clone = Arc::clone(&self.semantic_receiving_channel);

        // Lock the receiver (this avoids the temporary value issue)
        let mut rx = receiver_clone.lock().await;
        
        while let Some(user_request) = rx.recv().await {
          if let Ok(internal_request) = self.map_user_request(user_request) {
            self.sending_channel.send(internal_request);
          }
        }

        let message_allocation = self.cluster.run().await;

        match message_allocation {
          Ok(()) => {
            {}
          },
          Err(error) => {
            eprintln!("Cluster error: {error}");
          }
        }

      },
      Implementation::LAZY => {
        // let message_allocation = self.cluster.add_or_update_node(client_request);

        // match message_allocation {
        //   Ok(()) => {
        //     self.cluster.execute_communication(&self.message_output_target);
        //   },
        //   Err(error) => {
        //     eprintln!("Error: {error}");
        //   }
        // }
        println!("DAG data type is required to record requests and then resolve on something like collect/show/write");
      },
    }
  }

  pub fn map_user_request(&mut self, user_request: String) -> Result<String, ClusterExceptions> {

    let (operation, input) = user_request.split_once(" ").unwrap();

    match operation {
        "read-file" => {
            let request_string = self.cluster.read_data_from_file(input.to_string())?;
            return Ok(request_string);
        },
        _ => {
            return Err(ClusterExceptions::UnkownClientRequest { error_message: user_request.to_string() });
        }
    }
  }
}
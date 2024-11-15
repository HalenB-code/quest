use super::cluster::{self, Cluster};
use super::implementation::{MessageExecutionType, Implementation};

// Session Class
// The session is created to manage the overall execution of client requests
// The execution engine is the cluster with the nodes that are created and connected to it
// The execution model -- eager or lazy -- is determined in the configs and dictates how incoming requests are executed
// Incoming requests are captured through STDIN and passed to the session, which will use the execution type constant to implement the execution model

#[derive(Debug, Clone)]
pub struct Session {
  pub session_id: String,
  pub cluster: Cluster,
  pub implementation_type: Implementation,
  pub message_output_target: MessageExecutionType
}

// Need to add eager/lazy execution at the session level to enable streaming and batching
// Based on the selection a keyword would be sent to cluster to determine whether incoming requests
// should be implemented once received or as part of DAG
impl Session {
  // Add create method to tie session to cluster
  pub fn new(cluster: Cluster, implementation_type: Implementation, message_output_target: MessageExecutionType) -> Session {
    return Self {session_id: "999".to_string(), cluster: cluster.clone(), implementation_type: implementation_type, message_output_target: message_output_target}
  }
  // Session execution used to handle incoming client request and execute, either lazily or eagerly
  // Might need to add Cluster object to session_context as cluster is the container for the session that all nodes are connected to and all messages will emanate to/from
  pub fn session_execution(&mut self, client_request: String) {

    match self.implementation_type {
      Implementation::EAGER => {
        let message_allocation = self.cluster.add_or_update_node(client_request);

        match message_allocation {
          Ok(()) => {
            self.cluster.execute_communication(&self.message_output_target);
          },
          Err(error) => {
            eprintln!("Allocation of message in cluster failed with this error: {error}");
          }
        }
        
      },
      Implementation::LAZY => {
        println!("DAG data type is required to record requests and then resolve on something like collect/show/write")
      },
      _ => panic!("Implementation model not selected or not supported")
    }
  }
}


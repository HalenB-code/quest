use tokio::sync::mpsc;
use std::collections::{HashMap, BTreeMap};

use crate::session_resources::cli::ClusterCommand;
use crate::session_resources::implementation::ImplementationMode;
use crate::session_resources::transactions::{QueryPlanStatus, QueryPlanTypes, Transaction};
use crate::session_resources::message::{Message, message_serializer};
use crate::session_resources::cluster::{self, Cluster};
use crate::session_resources::exceptions::ClusterExceptions;

// Session Class
// The session is created to manage the overall execution of client requests
// The execution engine is the cluster with the nodes that are created and connected to it
// The execution model -- eager or lazy -- is determined in the configs and dictates how incoming requests are executed
// Incoming requests are captured through STDIN and passed to the session, which will use the execution type constant to implement the execution model

#[derive(Debug)]
pub struct Session {
  pub session_id: String,
  pub cluster: Cluster,
  pub cli_request_receiver: mpsc::Receiver<String>,
  pub response_receiving_channel: mpsc::Receiver<String>,
}

// Need to add eager/lazy execution at the session level to enable streaming and batching
// Based on the selection a keyword would be sent to cluster to determine whether incoming requests
// should be implemented once received or as part of DAG
impl Session {
  // Add create method to tie session to cluster
  pub fn new(cluster: Cluster, cli_receiver: mpsc::Receiver<String>, cluster_response_receiver: mpsc::Receiver<String>) -> Session {
    return Self {
      session_id: "999".to_string(), 
      cluster, 
      cli_request_receiver: cli_receiver,
      response_receiving_channel: cluster_response_receiver
    }
  }

  pub fn add_current_request_to_query_plan(&mut self, request: ClusterCommand) {
    let transaction_manager = self.cluster.transaction_manager.as_mut().unwrap();
    transaction_manager.query_plan.add_step(request);
  }

  pub fn get_executables(&mut self, execution_target: String) -> Vec<Message> {
    let transaction_manager = self.cluster.transaction_manager.as_mut().unwrap();
    let mut executables: Vec<Message> = Vec::new();

    match execution_target.as_str() {
      "all" => {
        for (_step_id, sub_steps) in transaction_manager.query_plan.query_plan_steps.iter_mut() {
          for (_substep_id, substep_details) in sub_steps.iter_mut() {
            let message_request = &mut substep_details.2;
            executables.push(message_request.clone());
          }
        }
      },
      _ => {
        for (_step_id, sub_steps) in transaction_manager.query_plan.query_plan_steps.iter_mut() {
          // TODO: Add check to ensure only 1 substep is returned for the execution target, if more than 1, return error to client
          let i = 0;
          for (_substep_id, substep_details) in sub_steps.iter_mut() {
            let message_request = &mut substep_details.2;
            if i == 0 {
              executables.push(message_request.clone());
            }
            else {
              break
            }
          }
        }
      }
    }

    return executables;
  }

  pub async fn execute_executables(&mut self, executables: Vec<Message>) -> Result<(), ClusterExceptions> {
    for executable in executables {
      let serialized_message = message_serializer(&executable);

      match serialized_message {
        Ok(serialized) => {
          self.cluster.categorize_and_queue(serialized).await?;
        },
        Err(error) => {
          return Err(ClusterExceptions::InvalidCommand { error_message: format!("Failed to serialize message: {:?}", error) });
        }
      }
    }
    Ok(())
  }

  // Session execution used to handle incoming client request and execute, either lazily or eagerly
  // Might need to add Cluster object to session_context as cluster is the container for the session that all nodes are connected to and all messages will emanate to/from
  pub async fn session_execution(&mut self) {

    // Set execution type boolean to determine if messages are executed lazily or eagerly
    let is_eager_execution: bool;

    match self.cluster.cluster_configuration.cluster_settings.execution_mode {
      ImplementationMode::EAGER => {
        is_eager_execution = true;
      },
      ImplementationMode::LAZY => {
        is_eager_execution = false;
      }
    };

    loop {
        tokio::select! {
            cli_request = self.cli_request_receiver.recv() => {

              // TODO: Add a check to see if the request is a valid command, if not, return an error message to the client
              let cli_mapped_request = ClusterCommand::map_command_line_request(cli_request.unwrap());
              let list_of_executables: Vec<Message>;

              match cli_mapped_request {

                Ok(mapped_request) => {

                  println!("Mapped request: {:?}", mapped_request);

                  self.add_current_request_to_query_plan(mapped_request.clone());

                  match is_eager_execution {

                    // Execute each step in QueryPlan immediately after it is added to the plan
                    true => {

                      list_of_executables = self.get_executables("".to_string());
                      self.execute_executables(list_of_executables).await.unwrap_or_else(|error| {
                        println!("Error executing request: {:?}", error);
                      });

                    }
                    false => {

                      match mapped_request {
                        ClusterCommand::CmdExecute { .. } => {
                          // TODO: Remove unwrap and handle error if transaction_manager is None
                          list_of_executables = self.get_executables("all".to_string());
                          self.execute_executables(list_of_executables).await.unwrap_or_else(|error| {
                            println!("Error executing request: {:?}", error);
                            });
                        },
                        _ => {
                          continue
                        }
                      }
                    }
                  }
                }
                Err(error) => {
                  println!("Error mapping request: {:?}", error);
                }
            }
            },
            cluster_message = self.response_receiving_channel.recv() => {

                match cluster_message {
                    Some(message) => {
                        if let Err(error) = self
                            .cluster
                            .categorize_and_queue(message)
                            .await
                        {
                            eprintln!(
                                "Cluster message failed: {error}"
                            );
                        }
                    }
                    None => {
                        println!(
                            "Cluster message channel closed"
                        );
                        break;
                    }
                }
            }
        }
    }
  }
}
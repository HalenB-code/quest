use tokio::sync::mpsc;

use crate::session_resources::cli::ClusterCommand;
use crate::session_resources::implementation::ImplementationMode;
use crate::session_resources::message::{Message, message_serializer};
use crate::session_resources::cluster::{Cluster};
use crate::session_resources::exceptions::ClusterExceptions;
use crate::session_resources::transactions::{QueryPlanStatus};
use crate::session_resources::message::MessageFields;

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
  pub node_event_receiver: mpsc::Receiver<Message>,
}

// Need to add eager/lazy execution at the session level to enable streaming and batching
// Based on the selection a keyword would be sent to cluster to determine whether incoming requests
// should be implemented once received or as part of DAG
impl Session {
  // Add create method to tie session to cluster
  pub fn new(cluster: Cluster, cli_receiver: mpsc::Receiver<String>, cluster_response_receiver: mpsc::Receiver<String>, node_event_receiver: mpsc::Receiver<Message>) -> Session {
    return Self {
      session_id: "999".to_string(), 
      cluster, 
      cli_request_receiver: cli_receiver,
      response_receiving_channel: cluster_response_receiver,
      node_event_receiver: node_event_receiver
    }
  }

  pub async fn add_current_request_to_query_plan(&mut self, request: ClusterCommand) -> Result<usize, ClusterExceptions> {
    let active_cluster_nodes = self.cluster.get_nodes().clone();
    let transaction_manager = self.cluster.transaction_manager.as_mut().unwrap();
    
    match transaction_manager.query_plan.add_step(&mut transaction_manager.file_system_manager, active_cluster_nodes, request).await {
      Ok(step_id) => {
        return Ok(step_id);
      },
      Err(error) => {
      return Err(ClusterExceptions::InvalidCommand { error_message: format!("Failed to add request to query plan: {:?}", error) });
      }
    }
  }

  pub fn get_executables(&mut self, execution_target: Option<usize>) -> Vec<(String, Message)> {
    let transaction_manager = self.cluster.transaction_manager.as_mut().unwrap();
    let mut executables: Vec<(String, Message)> = Vec::new();

    println!("Query plan steps: {:?}", transaction_manager.query_plan.query_plan_steps);
    match execution_target {

      Some(target_step_id) => {
        for (step_id, sub_steps) in transaction_manager.query_plan.query_plan_steps.iter_mut() {

          if &target_step_id == step_id {

            for (_substep_id, substep_details) in sub_steps.iter_mut() {

              let (message_id, _, message_request, message_status) = &substep_details;

              if message_status == &QueryPlanStatus::Pending {
                executables.push((message_id.to_string(), message_request.clone()));
              }
            }
          }
        }
      },
      None => {
        for (_step_id, sub_steps) in transaction_manager.query_plan.query_plan_steps.iter_mut() {
          for (_substep_id, substep_details) in sub_steps.iter_mut() {

            let (message_id, _, message_request, message_status) = &substep_details;

              if message_status == &QueryPlanStatus::Pending {
                  executables.push((message_id.to_string(), message_request.clone()));
              } else {
                continue
              }
          }
        }
      }
    }

    return executables;
  }

  pub async fn execute_executables(&mut self, query_plan_step_id: Option<usize>, executables: Vec<(String, Message)>) -> Result<(), ClusterExceptions> {
    
    for (executable_id, executable) in executables {
      let serialized_message = message_serializer(&executable);

      match serialized_message {
        Ok(serialized) => {
          let execution_result = self.cluster.categorize_and_queue(serialized).await;

          match execution_result {
            Ok(_) => {
              let transaction_manager = self.cluster.transaction_manager.as_mut().unwrap();
              transaction_manager.query_plan.update_step_status(query_plan_step_id, executable_id, QueryPlanStatus::Complete)?;
            },
            Err(_) => {
              let transaction_manager = self.cluster.transaction_manager.as_mut().unwrap();
              transaction_manager.query_plan.update_step_status(query_plan_step_id, executable_id, QueryPlanStatus::Failed)?;
            }
          };
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
              let list_of_executables: Vec<(String, Message)>;

              match cli_mapped_request {

                Ok(mapped_request) => {

                  println!("Mapped request: {:?}", mapped_request);
                  let target_step_id: usize;

                  match self.add_current_request_to_query_plan(mapped_request.clone()).await {
                      Ok(step_id) => {
                        target_step_id = step_id;
                      }
                      Err(error) => {
                        eprintln!("Error adding request to query plan: {:?}", error);
                        continue;
                      }
                  }

                  match is_eager_execution {

                    // Execute each step in QueryPlan immediately after it is added to the plan
                    true => {

                      list_of_executables = self.get_executables(Some(target_step_id));
                      println!("Executing request: {:?}", list_of_executables);
                      self.execute_executables(Some(target_step_id), list_of_executables).await.unwrap_or_else(|error| {
                        println!("Error executing request: {:?}", error);
                      });

                    }
                    false => {

                      match mapped_request {
                        ClusterCommand::CmdExecute { .. } => {
                          // TODO: Remove unwrap and handle error if transaction_manager is None
                          list_of_executables = self.get_executables(None);
                          self.execute_executables(None, list_of_executables).await.unwrap_or_else(|error| {
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
            },
            node_event = self.node_event_receiver.recv() => {
              match node_event {
                Some(message) => {
                    match message {
                        Message::Request { .. }
                        | Message::Init { .. } => {
                            if let Err(error) = self
                                .cluster
                                .propagate_message(message)
                                .await
                            {
                                eprintln!(
                                    "Failed to propagate node message: {error}"
                                );
                            }
                        }

                        Message::Response { .. } => {
                            if let Err(error) = self
                                .cluster
                                .messenger
                                .response_queue(message)
                                .await
                            {
                                eprintln!(
                                    "Failed to queue node response: {error}"
                                );
                            }
                        }
                    }
                }
                None => {
                    println!("Node event channel closed");
                }
              }
            }
        }
    }
  }
}
use serde_json::{Result, Value, Error};
use std::io;
use std::collections::{BTreeMap, HashMap};
use serde::{Serialize, Deserialize};
use serde::de::{self, Deserializer, MapAccess, Visitor};



// Message Handler Class
// Without deserializing an incoming message, the destination node which needs to produce the response will never be known
// Therefore a Message Handler is required to receive incoming requests -- these could be messages to STDIN or Session requests --
// and then route the message request to the correct node

// Deserializer
fn message_deserializer(input_string: String) -> Result<Message> {
    return serde_json::from_str(&input_string.trim());
}

// Serializer
fn message_serializer(output_string: &String) -> Result<String> {
  return serde_json::to_string(output_string);
}

// Message Enum
#[derive(Deserialize, Debug, Serialize)]
enum Message {
  Init {  msg_type: String, msg_id: usize, node_id: String, node_ids: Vec<String>},
  Request {  src: String, dest: String, body: MessageType },
  Response {  src: String, dest: String, body: MessageType },
}

// Message SubType: Init
#[derive(Deserialize, Debug, Serialize)]
struct Init {
  #[serde(rename = "type")]
  msg_type: String,
  msg_id: usize,
  node_id: String,
  node_ids: Vec<String>
}

// Note on Request/Response structs
// These two structs are different sides of the same coin: for every request to a node there should be a response from a node
// Therefore an impl on Response should take in a Request and produce a Response; without the one there cannot be the other

// Message SubType: Request
#[derive(Deserialize, Debug, Serialize)]
struct Request {
  src: String,
  dest: String,
  body: MessageType,
}

// Message SubType: Response
#[derive(Deserialize, Debug, Serialize)]
struct Response {
  src: String,
  dest: String,
  body: MessageType,
}

impl Response {
  !todo("Add impl that creates a Response from a Request message");
}

// MessageType Class
// MessageType is the content of the Message
// Message is functional object of sending and receiving a message while MessageType determines the nature of what that message will say
#[derive(Deserialize, Debug, Serialize)]
enum MessageType {
  Echo {typ: String, msg_id: usize, echo: String},
  EchoOk {typ: String, msg_id: usize, in_reply_to: usize, echo: String},
  // Broadcast,
  // Heartbeat,
}

#[derive(Deserialize, Debug, Serialize)]
struct Echo {
  #[serde(rename = "type")]
  typ: String,
  msg_id: usize,
  echo: String,
}

#[derive(Deserialize, Debug, Serialize)]
struct EchoOk {
  #[serde(rename = "type")]
  typ: String,
  msg_id: usize,
  in_reply_to: usize,
  echo: String,
}

// Implement custom deserialization for Message
// Note that the deserialization message type will only ever be Init or Request as these are messages coming in from STDIN encoded in JSON
impl<'de> Deserialize<'de> for Message {
  fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
  where
      D: Deserializer<'de>,
  {
      // Deserialize the input into a generic map first to inspect fields
      let map: serde_json::Value = Deserialize::deserialize(deserializer)?;

      // Check for type attr
      if map.get("type").is_some() {
        // If type attr present, check if type == init
        match map.get("type").and_then(|t| t.as_str()) {
          // Deserialize to Init
          Some("init") => {
            let init_request: Init = serde_json::from_value(map).map_err(de::Error::custom)?;
            Ok(Message::Init {
                msg_type: init_request.msg_type,
                msg_id: init_request.msg_id,
                node_id: init_request.node_id,
                node_ids: init_request.node_ids 
            })
          }
        }

      } else if map.get("src").is_some() && map.get("dest").is_some() {
          !todo("Add deserialization for MessageType to set enum on body");
          // Deserialize to Request
          let request_response_message: Request = serde_json::from_value(map).map_err(de::Error::custom)?;
          Ok(Message::Request {
              src: request_response_message.src,
              dest: request_response_message.dest,
              body: request_response_message.body
          })
      } else {
          Err(de::Error::custom("Unknown variant or missing fields"))
      }
  }
}

impl Serialize for Message {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
      S: Serializer,
  {
      match self {
          Message::Init { msg_type, msg_id, node_id, node_ids } => {
              let mut state = serializer.serialize_struct("Init", 4)?;
              state.serialize_field("msg_type", msg_type)?;
              state.serialize_field("msg_id", msg_id)?;
              state.serialize_field("node_id", node_id)?;
              state.serialize_field("node_ids", node_ids)?;
              state.end()
          },
          Message::Request { src, dest, body } => {
              let mut state = serializer.serialize_struct("Request", 3)?;
              state.serialize_field("src", src)?;
              state.serialize_field("dest", dest)?;
              state.serialize_field("body", body)?;
              state.end()
          },
          Message::Response { src, dest, body } => {
              let mut state = serializer.serialize_struct("Response", 3)?;
              state.serialize_field("src", src)?;
              state.serialize_field("dest", dest)?;
              state.serialize_field("body", body)?;
              state.end()
          }
      }
  }
}

// Custom deserialization for SubVariant to handle "type" field
impl<'de> Deserialize<'de> for MessageType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let map: Value = Deserialize::deserialize(deserializer)?;

        // Match on "type" to determine which variant to deserialize
        match map.get("type").and_then(|t| t.as_str()) {
            Some("echo") => {
                let data: Echo = serde_json::from_value(map).map_err(de::Error::custom)?;
                Ok(MessageType::Echo {
                    typ: data.typ,
                    msg_id: data.msg_id,
                    echo: data.echo
                })
            },
            // Some("broadcast") => {
            //     // Deserialize AnotherStruct variant
            //     #[derive(Deserialize)]
            //     struct AnotherStructData {
            //         another_field: String,
            //     }

            //     let data: AnotherStructData = serde_json::from_value(map).map_err(de::Error::custom)?;
            //     Ok(SubVariant::AnotherStruct {
            //         another_field: data.another_field,
            //     })
            // },
            _ => Err(de::Error::custom("Unknown variant for SubVariant")),
        }
    }
}

impl Serialize for MessageType {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
      S: Serializer,
  {
      match self {
          MessageType::Echo { typ , msg_id, echo } => {
              let mut state = serializer.serialize_struct("Echo", 3)?;
              state.serialize_field("typ", typ)?;
              state.serialize_field("msg_id", msg_id)?;
              state.serialize_field("echo", echo)?;
              state.end()
          }
          MessageType::EchoOk { typ , msg_id, in_reply_to, echo } => {
              let mut state = serializer.serialize_struct("EchoOk", 4)?;
              state.serialize_field("typ", typ)?;
              state.serialize_field("msg_id", msg_id)?;
              state.serialize_field("in_reply_to", in_reply_to)?;
              state.serialize_field("echo", echo)?;
              state.end()
          }
      }
  }
}


// Node Class
// Node is the execution unit of a cluster
struct Node {
  node_id: usize,
  node_ids: Vec<usize>,
  message_requests: VecDeque<Request>,
  message_responses: VecDeque<Response>,
  message_record: HashMap<usize, String>
}

impl Node {
  // Create new instance of Node if not already in existance; existance determined by entry in nodes collection as multiple nodes may be created
  fn new(message: Message, cluster: Cluster) -> self {
    // Init enum used to determine whether incoming message is Init or RequestResponse
    if message == Init {
      let node_exists = Cluster.nodes.contains_key(message.node_id);

      match node_exists {
        false => {
          let new_node = self { node_id: message.node_id, node_ids: message.node_ids, message_requests: VecDeque::new(), message_responses: VecDeque::new(), message_record: HashMap::new() };
          let node_add: Result<(), ClusterExceptions> = Cluster.add(new_node.node_id);

          match node_add {
            // Existing node prints and return Self
            Ok(()) => {
              println!("Node {} has been connected to {}", node_exists, Cluster.cluster_id);
              self
            },
            Err(node_add) => {
              // New node  prints and appends Self to nodes collection
              println!("{} exception on connecting {} to {}", node_exists, new_node.node_id, Cluster.cluster_id); 
              self 
            }
          }
        },
        true => {
          println!("Node {} already exists and connected to {}", message.node_id, Cluster.cluster_id);

          let retrieve_node = Cluster.nodes.get_key_value(message.node_id);

          match retrieve_node {
            Some(retrieve_node) => {
              retrieve_node.clone().1
            },
            None => panic!("Node {} exists but failed to retrieve object from cluster")
          }
        }
      }
    }
  }
  // Update existing Node with new message
  fn or_update(self, message: Message) -> self {
    // Only update if Node exists and message is RequestResponse
    if message == RequestResponse {

      let node_exists: Option<String> = nodes.get(message.node_id);

      match node_exists {
        // When Node does exist, update and return self
        Some(node_exists) => {
          println!("Node {} already exists", node_exists.unwrap()); 
          let mut node = self;
          let node_message_reference = nodes.get(node).unwrap().message_register.len() + 1;

          nodes.entry(node)
          .and_modify(node.message_register.insert(node_message_reference, message))
        },
        None => {
          panic!("Node {} does not exist so message request cannot be executed", node_exists.unwrap());
        }
      }
    }
  }

}

// Cluster Class
// Collection of nodes that will interact to achieve a task
struct Cluster {
  cluster_id: String,
  nodes: BTreeMap<usize, Node>,
  node_message_log: Hashmap<usize, Message>
}

enum ClusterExceptions {
  NodeAlreadyAttachedToCluster,
  NodeNotAttachedToCluster,
  FailedToRemoveNodeFromCluster,
}

impl Cluster {

  fn create(cluster_reference: usize) -> self {
    // TODO: check whether cluster with reference already exists
    self { cluster_id: format!("cluster-{cluster_reference}"), nodes: BTreeMap::new(), node_message_log: HashMap::new() }
  }

  fn add(self, node: Node) -> Result<(), ClusterExceptions> {
    let node_entry = self.get(Node.node_id);

    match node_entry {
      Some(node_entry) => {println!("Node {} already connected to {}", Node.node_id, self.cluster_id); ClusterExceptions::NodeAlreadyAttachedToCluster},
      None => {
        self.insert(Node.node_id, Node); 
        println!("{} has been connected to {}", Node.node_id, self.cluster_id); 
        Ok(())
      }
    }
  }

  fn remove(self, node: Node) -> Result<(), ClusterExceptions>{
    let node_entry = self.get(Node.node_id,);

    match node_entry {
      Some(node_entry) => {
        let node_removal = self.remove(Node.node_id);

        match node_removal {
          Some(node_removal) => {
            println!("{} has been disconnected from {}", Node.node_id, self.cluster_id); 
            Ok(())
          },
          None => {
            println!("Failed to disconnect {} from {}", Node.node_id, self.cluster_id); 
            ClusterExceptions::FailedToRemoveNodeFromCluster
          }
        }
        
      },
      None => {
        println!("Node {} not connected to {}", Node.node_id, self.cluster_id); 
        ClusterExceptions::NodeNotAttachedToCluster
      }
    }
  }

  fn terminate(self) {
    let node_count = self.is_empty();

    match node_count {
      true => {println!("{} does not have any nodes allocated", self.cluster_id)},
      false => {
        for key in self.get_nodes() {
          println!("Removing {} from {}", key, self.cluster_id);
          self.remove(key)
        }
        println!("All nodes removed from {}", self.cluster_id);
        drop(self);
      }
    }
  }

  fn get_nodes(self) {
    self.keys()
  }

  fn count_nodes() {
    self.len()
  }

  fn execute_communication() {
    // For each node in cluster

    for (node_id, node) in self.nodes {

    }
    // Iterate through message_requests

    // Append each request to Cluster log

    // Create a response for matching request (i.e. Echo => EchoOk)

    // Append response to Cluster log

    // Execute response according to the execution type
  }
}

enum MessageExecutionType {
  StdOut,
  TCP // This will support nodes on separate machines
}

struct StdOut {}

impl StdOut {
  fn write_to_std_out(message: String) -> Result<()> {
    let mut stdout = io::stdout().lock();

    stdout.write_all(format!("b{message}"))?;

    Ok(())
  }
}

enum MessageExecutionTypeErrors {
  FailedToExecuteMessageResponse
}

impl MessageExecutionType {
  fn execute_response(self) -> Result<MessageExecutionTypeErrors> {
    match self {
      StdOut,
    }
  }
}

// Session Class
// The session is created to manage the overall execution of client requests
// The execution engine is the cluster with the nodes that are created and connected to it
// The execution model -- eager or lazy -- is determined in the configs and dictates how incoming requests are executed
// Incoming requests are captured through STDIN and passed to the session, which will use the execution type constant to implement the execution model

struct Session {
  session_id: String,
  cluster: Cluster
}

// Need to add eager/lazy execution at the session level to enable streaming and batching
// Based on the selection a keyword would be sent to cluster to determine whether incoming requests
// should be implemented once received or as part of DAG
impl Session {
  // Add create method to tie session to cluster
  todo!("create method")
  // Session execution used to handle incoming client request and execute, either lazily or eagerly
  // Might need to add Cluster object to session_context as cluster is the container for the session that all nodes are connected to and all messages will emanate to/from
  fn session_execution(implementation_model: Implementation, cluster_id: Cluster, client_request: String) -> () {
    loop {
      let client_request: Result<Message> = message_deserializer(std_input);
  
      match client_request {
        Ok(request) => {

          // Depending on the execution model, execution eagerly or lazily

        },
        // Error out of client request that has no execution context (i.e. does not match internally supported actions)
        Err(error) => println!("Error reading from STDIN {error}"),
      }
    }

  }

}

// These implementations need to be back by a implementation model that reflects the type
enum Implementation {
  EAGER,
  LAZY
}

fn main() {

  let implementation: String = Implementation::EAGER;
  let cluster: Cluster = Cluster::create(1);
  let session = Session::session_context(implementation_model, cluster);
  
  loop {
    let mut std_input = String::new();

    match io::stdin().read_line(&mut std_input) {
      Ok(line) => {

        // If next STDIN message is exit, break from loop and return from session
        if line.to_string().to_lowercase() == "exit" {
          break;
        }

        // Otherwise, deserialize message into one of expected types and continue loop
        else {        
          match implementation {
            Implementation::EAGER => {
              session_execution(cluster, line);
            },
            Implementation::LAZY => {
            todo!("DAG data type is required to record requests and then resolve on something like collect/show/write")
            },
            _ => panic!("Implementation model not selected or not supported")
          }
        }
      },
      Err(error) => println!("Error reading from STDIN {error}"),
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

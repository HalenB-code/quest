pub mod session;
pub mod session_resources;

use std::env;
use components::resource_factory::*;





fn main() {

  let args: Vec<String> = env::args().collect();

  let resource_requested = &args[1];

  if resource_requested == "coordinator" {
    let resource = Resource { variant: ResourceType::Coordinator };
    resource.initiate_response();
  }
  else {
    let resource = Resource { variant: ResourceType::Agent };
    resource.initiate_response();
  }

  println!("Main end!");
}



use serde_json::{Result, Value, Error};
use std::io;

struct NodeMessage {
  source: String,
  destination: String,
  typ: String,
  message_id: u64,
  echo: String
}

impl NodeMessage {
  fn new(input_string: &String) -> Result<NodeMessage> {
    let v  = serde_json::from_str::<Value>(&input_string);
    println!("{:?}", v);
    match v {
      Ok(v) => {
        Ok(NodeMessage {source: v["src"].to_string(), destination: v["dest"].to_string(), typ: v["body"]["type"].to_string(), message_id: v["body"]["msg_id"].as_u64().unwrap(), echo: v["body"]["echo"].to_string()})
      },
      Err(Error) => {
        println!("There was an error");
        Err(Error)
      }
    }
    
  }
}
  
fn main() {
    let data = r#"
            {
              "src": "c1",
              "dest": "n1",
              "body": {
                "type": "echo",
                "msg_id": 1,
                "echo": "Please echo 35"
              }
    }"#;
    
    let new_node: Result<NodeMessage> = NodeMessage::new(&data.to_string());
    
    match new_node {
        Ok(new_node) => println!("{}", new_node.source),
        Err(Error) => println!("Some error occured")
    }
    
    println!("All done from main!");
            
}
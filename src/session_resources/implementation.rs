use std::io::StdoutLock;
use std::io::{Write, Error};


// These implementations need to be back by a implementation model that reflects the type

#[derive(Debug, Clone)]
pub enum Implementation {
    EAGER,
    LAZY
  }
  
//   pub struct LAZY {
//     // Incoming requests from client
//     pub client_requests: Vec<String>,
//     // Incoming requests will be parsed for keywords and logical actions, i.e. groupby().collect()
//     pub keywords: Vec<String>,
//     // An execution plan must be developed based on keywords and logical actions and must be distributed across nodes in cluster
//     // Each individual nodes part of the execution plan will then be sent to it for processing
//   }

#[derive(Debug, Clone)]
pub enum MessageExecutionType {
    StdOut,
    TCP // This will support nodes on separate machines
  }
  
#[derive(Debug, Clone)]
pub struct StdOut {}

#[derive(Debug, Clone)]
pub struct TCP {}
  
impl StdOut {
pub fn write_to_std_out(mut stdout: StdoutLock<'static>, message: String) -> Result<(), std::io::Error> {
    let bytes = message.as_bytes();
    stdout.write_all(bytes)?;
    stdout.flush();
    Ok(())
}
}

pub enum MessageExecutionTypeErrors {
    FailedToExecuteMessageResponse
}

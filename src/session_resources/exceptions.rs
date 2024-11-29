use std::io;
use serde_json;
use std::fmt;

#[derive(Debug)]
pub enum ClusterExceptions {
    IOError(io::Error),
    JsonError(serde_json::Error),
    NodeDoesNotExist { error_message: String },
    NodeAlreadyAttachedToCluster { error_message: String },
    FailedToRemoveNodeFromCluster { error_message: String },
    UnkownClientRequest { error_message: String },
    ClusterDoesNotHaveAnyNodes { error_message: String },
    FailedToRetrieveNodeFromCluster { error_message: String },
    ClusterMessengerFailedToFetchRequest { error_message: String },
    ClusterMessengerFailedToReQueueReceivedRequest { error_message: String },
    // Add other error types here
}

// Implement `From` for `io::Error`
impl From<io::Error> for ClusterExceptions {
    fn from(error: io::Error) -> Self {
        ClusterExceptions::IOError(error)
    }
}

// Implement `From` for `serde_json::Error`
impl From<serde_json::Error> for ClusterExceptions {
    fn from(error: serde_json::Error) -> Self {
        ClusterExceptions::JsonError(error)
    }
}

impl fmt::Display for ClusterExceptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClusterExceptions::IOError(error_message) => write!(f, "IO error occurred: {}", error_message),
            ClusterExceptions::JsonError(error_message) => write!(f, "JSON error occurred: {}", error_message),
            ClusterExceptions::NodeDoesNotExist{ error_message } => {
                write!(f, "Node '{}' does not exist. Please init node and try again.", error_message)
            },
            ClusterExceptions::NodeAlreadyAttachedToCluster { error_message } => {
                write!(f, "Node '{}' already attached to cluster.", error_message)
            },
            ClusterExceptions::FailedToRemoveNodeFromCluster { error_message } => {
                write!(f, "Failed to remove Node '{}' from cluster. Please try again.", error_message)
            },
            ClusterExceptions::UnkownClientRequest { error_message } => {
                write!(f, "Unkown client request '{}'. Please confirm nature of the request and try again.", error_message)
            },
            ClusterExceptions::ClusterDoesNotHaveAnyNodes { error_message } => {
                write!(f, "Cluster {} does not have any nodes to terminate.", error_message)
            },
            ClusterExceptions::FailedToRetrieveNodeFromCluster { error_message } => {
                write!(f, "Failed to retrieve Node '{}' from the cluster.", error_message)
            },
            ClusterExceptions::ClusterMessengerFailedToFetchRequest { error_message } => {
                write!(f, "Failed to retrieve request '{}' from the cluster messenger.", error_message)
            },
            ClusterExceptions::ClusterMessengerFailedToReQueueReceivedRequest { error_message } => {
                write!(f, "Failed to requeue message '{}' dervied from initial request.", error_message)
            }
        }
    }
}

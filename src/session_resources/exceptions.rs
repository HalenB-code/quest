use std::io;
use serde_json;
use std::fmt;
use crate::session_resources::message::{Message, MessageExceptions};
use crate::session_resources::transactions::TransactionExceptions;
use crate::session_resources::file_system::FileSystemExceptions;

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
    ClusterReceivingChannelSendError { error_message: String },
    NodeFailedToCreateDataStore { error_message: String },
    NodeDataStoreObjectNotAvailable { error_message: String },
    InvalidClusterRequest { error_message_1: Message, error_message_2: String },
    MessageError(MessageExceptions),
    TransactionError(TransactionExceptions),
    ConfigError(TransactionExceptions),
    FileSystemError(FileSystemExceptions)
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

// Implement `From` for `serde_json::Error`
impl From<MessageExceptions> for ClusterExceptions {
    fn from(error: MessageExceptions) -> Self {
        ClusterExceptions::MessageError(error)
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
            },
            ClusterExceptions::ClusterReceivingChannelSendError { error_message } => {
                write!(f, "Failed to requeue message '{}' dervied from initial request.", error_message)
            },
            ClusterExceptions::NodeFailedToCreateDataStore { error_message } => {
                write!(f, "Node '{}' failed to create datastore.", error_message)
            },
            ClusterExceptions::NodeDataStoreObjectNotAvailable { error_message } => {
                write!(f, "The request datastore object is not yet avaialble for Node '{}'.", error_message)
            },
            ClusterExceptions::InvalidClusterRequest { error_message_1, error_message_2 } => {
                write!(f, "Request {:?} for Node '{}' is not valid.", error_message_1, error_message_2)
            },
            ClusterExceptions::MessageError(error_message) => {
                write!(f, "Message Error occurred: '{}'", error_message)
            },
            ClusterExceptions::TransactionError(error_message) => {
                write!(f, "Transaction Error occurred: '{}'", error_message)
            },
            ClusterExceptions::ConfigError(error_message) => {
                write!(f, "Config Error occurred: '{}'", error_message)
            },
            ClusterExceptions::FileSystemError(error_message) => {
                write!(f, "File System Error occurred: '{}'", error_message)
            },
        }
    }
}

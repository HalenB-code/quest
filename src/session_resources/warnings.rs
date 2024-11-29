use std::fmt;

#[derive(Debug)]
pub enum ClusterWarnings {
    NoNewRequestsToProcess { warning_message: String }
    // Add other error types here
}

impl fmt::Display for ClusterWarnings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {

            ClusterWarnings::NoNewRequestsToProcess { warning_message } => {
                write!(f, "No new requests to process for cluster '{}'.", warning_message)
            }
        }
    }
}

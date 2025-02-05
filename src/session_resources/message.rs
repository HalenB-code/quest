use serde::{Serialize, Deserialize};
use serde_json::Result;
use std::fmt;
use std::collections::HashMap;
use crate::session_resources::file_system::FileSystemType;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged, rename_all = "lowercase")]
pub enum Message {
    Init {
        #[serde(rename = "type")]
        msg_type: String,
        msg_id: usize,
        node_id: String,
        node_ids: Vec<String>,
    },
    Request {
        src: String,
        dest: String,
        
        // #[serde(with = "as_json_string")]
        body: MessageType,
    },
    Response {
        src: String,
        dest: String,
        
        // #[serde(with = "as_json_string")]
        body: MessageType,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum MessageType {
    Echo {
        msg_id: usize,
        echo: String,
    },
    #[serde(rename = "echo_ok")]
    EchoOk {
        msg_id: usize,
        in_reply_to: usize,
        echo: String,
    },
    Generate {
    },
    #[serde(rename = "generate_ok")]
    GenerateOk {
        msg_id: usize,
        in_reply_to: usize,
        id: String
    },
    Broadcast {
        message: usize
    },
    #[serde(rename = "broadcast_ok")]
    BroadcastOk {
        msg_id: usize,
        in_reply_to: usize,
    },
    // This will clash with other read requests for later challenges
    #[serde(rename = "broadcast_read")]
    BroadcastRead {
    },
    #[serde(rename = "broadcast_read_ok")]
    BroadcastReadOk {
        messages: Vec<usize>
    },
    Topology {
        topology: HashMap<String, Vec<String>>
    },
    #[serde(rename = "topology_ok")]
    TopologyOk {
    },    #[serde(rename = "add")]
    VectorAdd {
        delta: String
    },
    #[serde(rename = "add_ok")]
    VectorAddOk {
    },
    #[serde(rename = "read")]
    VectorRead {
    },
    #[serde(rename = "read_ok")]
    VectorReadOk {
        value: String
    },    
    Send {
        key: String,
        msg: String
    },
    #[serde(rename = "send_ok")]
    SendOk {
        offset: usize
    },    
    Poll {
        offsets: HashMap<String, usize>
    },
    #[serde(rename = "commit_offsets")]
    PollOk {
        // Structure in return is String "k1" => [[offset, value], [offset, value]]
        msgs: HashMap<String, Vec<Vec<usize>>>
    },
    #[serde(rename = "commit_offsets")]
    CommitOffsets {
        offsets: HashMap<String, usize>
    },
    #[serde(rename = "commit_offsets_ok")]
    CommitOffsetsOk {
    },
    #[serde(rename = "list_commited_offsets")]
    ListCommitedOffsets {
        keys: Vec<String>
    },
    #[serde(rename = "list_commited_offsets_ok")]
    ListCommitedOffsetsOk {
        offsets: HashMap<String, usize>
    },
    #[serde(rename = "txn")]
    Transaction {
        txn: Vec<Vec<String>>
    },
    #[serde(rename = "txn_ok")]
    TransactionOk {
        txn: Vec<Vec<String>>
    },
    KeyValueRead {
        key: String
    },
    KeyValueReadOk {
    },
    KeyValueWrite {
        key: String,
        value: String
    },
    KeyValueWriteOk {
    },
    GlobalCounterRead {
        key: String,
        value: String
    },
    GlobalCounterReadOk {
    },
    GlobalCounterWrite {
        key: String,
        value: String
    },
    GlobalCounterWriteOk {
    },
    ReadFromFile {
        file_path: String,
        accessibility: String,
        bytes: String,
        schema: String
    },
    ReadFromFileOk{
    }
}

// Module to handle serialization and deserialization as a JSON string.
mod as_json_string {
    use serde::{Serialize, Deserialize, Serializer, Deserializer};
    use serde::de::DeserializeOwned;
    use serde_json;

    pub fn serialize<T, S>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        T: Serialize,
        S: Serializer,
    {
        use serde::ser::Error;
        let json_str = serde_json::to_string(value).map_err(Error::custom)?;
        serializer.serialize_str(&json_str)
    }

    pub fn deserialize<'de, T, D>(deserializer: D) -> Result<T, D::Error>
    where
        T: DeserializeOwned,
        D: Deserializer<'de>,
    {
        use serde::de::Error;
        let json_str = String::deserialize(deserializer)?;
        serde_json::from_str(&json_str).map_err(Error::custom)
    }
}

// Deserializer for Message
pub fn message_deserializer(input_string: &String) -> Result<Message> {
    serde_json::from_str(input_string)
}

// Serializer for Message
pub fn message_serializer(output_message: &Message) -> Result<String> {
    serde_json::to_string(output_message)
}

impl fmt::Display for MessageType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MessageType::Echo { .. } => write!(f, "Echo"),
            MessageType::EchoOk { .. } => write!(f, "EchoOk"),
            MessageType::Generate { .. } => write!(f, "Generate"),
            MessageType::GenerateOk { .. } => write!(f, "GenerateOk"),
            MessageType::Broadcast { .. } => write!(f, "Broadcast"),
            MessageType::BroadcastOk { .. } => write!(f, "BroadcastOk"),
            MessageType::BroadcastRead { .. } => write!(f, "BroadcastRead"),
            MessageType::BroadcastReadOk { .. } => write!(f, "BroadcastReadOk"),
            MessageType::Topology { .. } => write!(f, "Topology"),
            MessageType::TopologyOk { .. } => write!(f, "TopologyOk"),
            MessageType::VectorAdd { .. } => write!(f, "VectorAdd"),
            MessageType::VectorAddOk { .. } => write!(f, "VectorAddOk"),
            MessageType::VectorRead { .. } => write!(f, "VectorRead"),
            MessageType::VectorReadOk { .. } => write!(f, "VectorReadOk"),
            MessageType::Send { .. } => write!(f, "Send"),
            MessageType::SendOk { .. } => write!(f, "SendOk"),
            MessageType::Poll { .. } => write!(f, "Poll"),
            MessageType::PollOk { .. } => write!(f, "PollOk"),
            MessageType::CommitOffsets { .. } => write!(f, "CommitOffsets"),
            MessageType::CommitOffsetsOk { .. } => write!(f, "CommitOffsetsOk"),
            MessageType::ListCommitedOffsets { .. } => write!(f, "ListCommitedOffsets"),
            MessageType::ListCommitedOffsetsOk { .. } => write!(f, "ListCommitedOffsetsOk"),
            MessageType::Transaction { .. } => write!(f, "Transaction"),
            MessageType::TransactionOk { .. } => write!(f, "TransactionOk"),
            MessageType::KeyValueRead { .. } => write!(f, "KeyValueRead"),
            MessageType::KeyValueReadOk { .. } => write!(f, "KeyValueReadOk"),
            MessageType::KeyValueWrite { .. } => write!(f, "KeyValueRead"),
            MessageType::KeyValueWriteOk { .. } => write!(f, "KeyValueReadOk"),
            MessageType::GlobalCounterRead { .. } => write!(f, "GlobalCounterRead"),
            MessageType::GlobalCounterReadOk { .. } => write!(f, "GlobalCounterReadOk"),
            MessageType::GlobalCounterWrite { .. } => write!(f, "GlobalCounterWrite"),
            MessageType::GlobalCounterWriteOk { .. } => write!(f, "GlobalCounterWriteOk"),
            MessageType::ReadFromFile { .. } => write!(f, "ReadFromFile"),
            MessageType::ReadFromFileOk { .. } => write!(f, "ReadFromFileOk"),            
        }
    }
}

// Trait to access fields in `Message`
pub trait MessageFields {
    fn msg_type(&self) -> Option<&String>;
    fn msg_id(&self) -> Option<usize>;
    fn node_id(&self) -> Option<&String>;
    fn node_ids(&self) -> Option<&[String]>;
    fn src(&self) -> Option<&String>;
    fn dest(&self) -> Option<&String>;
    fn body(&self) -> Option<&MessageType>;

    fn set_msg_type(&mut self, msg_type: String);
    fn set_msg_id(&mut self, msg_id: usize);
    fn set_node_id(&mut self, node_id: String);
    fn set_node_ids(&mut self, node_ids: Vec<String>);
    fn set_src(&mut self, src: String);
    fn set_dest(&mut self, dest: String);
    fn set_body(&mut self, body: MessageType);
}

// Trait to access fields in `MessageType`
pub trait MessageTypeFields {
    fn msg_id(&self) -> Option<usize>;
    fn echo(&self) -> Option<&String>;
    fn in_reply_to(&self) -> Option<usize>;
    fn broadcast_msg(&self) -> Option<usize>;
    fn node_own_topology(&self) -> Option<&HashMap<String, Vec<String>>>;
    fn delta(&self) -> Option<&String>;
    fn kv_key(&self) -> Option<&String>;
    fn kv_value(&self) -> Option<&String>;
    fn offsets(&self) -> Option<&HashMap<String, usize>>;
    fn keys(&self) -> Option<&Vec<String>>;
    fn txn(&self) -> Option<&Vec<Vec<String>>>;
    fn file_path(&self) -> Option<&String>;
    fn file_system_type(&self) -> Option<&String>;

    fn set_txn(&mut self, new_txn: Vec<Vec<String>>);
}

// Implement `MessageFields` for `Message`
impl MessageFields for Message {

    fn msg_type(&self) -> Option<&String> {
        if let Message::Init { msg_type, .. } = self {
            Some(msg_type)
        } else {
            None
        }
    }

    fn msg_id(&self) -> Option<usize> {
        if let Message::Init { msg_id, .. } = self {
            Some(*msg_id)
        } else {
            None
        }
    }

    fn node_id(&self) -> Option<&String> {
        if let Message::Init { node_id, .. } = self {
            Some(node_id)
        } else {
            None
        }
    }

    fn node_ids(&self) -> Option<&[String]> {
        if let Message::Init { node_ids, .. } = self {
            Some(node_ids)
        } else {
            None
        }
    }

    fn src(&self) -> Option<&String> {
        match self {
            Message::Request { src, .. } | Message::Response { src, .. } => Some(src),
            _ => None,
        }
    }

    fn dest(&self) -> Option<&String> {
        match self {
            Message::Request { dest, .. } | Message::Response { dest, .. } => Some(dest),
            _ => None,
        }
    }

    fn body(&self) -> Option<&MessageType> {
        match self {
            Message::Request { body, .. } | Message::Response { body, .. } => Some(body),
            _ => None,
        }
    }

    fn set_msg_type(&mut self, update_msg_type: String) {
        if let Message::Init { ref mut msg_type, .. } = self {
            *msg_type = update_msg_type;
        }
    }

    fn set_msg_id(&mut self, update_msg_id: usize) {
        if let Message::Init { ref mut msg_id, .. } = self {
            *msg_id = update_msg_id;
        }
    }

    fn set_node_id(&mut self, update_node_id: String) {
        if let Message::Init { ref mut node_id, .. } = self {
            *node_id = update_node_id;
        }
    }

    fn set_node_ids(&mut self, update_node_ids: Vec<String>) {
        if let Message::Init { ref mut node_ids, .. } = self {
            *node_ids = update_node_ids;
        }
    }

    fn set_src(&mut self, update_src: String) {
        match self {
            Message::Request { ref mut src, .. } | Message::Response { ref mut src, .. } => {
                *src = update_src
            }
            _ => {}
        }
    }

    fn set_dest(&mut self, update_dest: String) {
        match self {
            Message::Request { ref mut dest, .. } | Message::Response { ref mut dest, .. } => {
                *dest = update_dest
            }
            _ => {}
        }
    }

    fn set_body(&mut self, update_body: MessageType) {
        match self {
            Message::Request { ref mut body, .. } | Message::Response { ref mut body, .. } => {
                *body = update_body
            }
            _ => {}
        }
    }
}

// Implement `MessageTypeFields` for `MessageType`
impl MessageTypeFields for MessageType {
    fn msg_id(&self) -> Option<usize> {
        match self {
            MessageType::Echo { msg_id, .. } | MessageType::EchoOk { msg_id, .. } => Some(*msg_id),
            MessageType::GenerateOk { msg_id, .. } => Some(*msg_id),
            MessageType::BroadcastOk { msg_id, .. } => Some(*msg_id),
            _ => None,
        }
    }

    fn echo(&self) -> Option<&String> {
        match self {
            MessageType::Echo { echo, .. } | MessageType::EchoOk { echo, .. } => Some(echo),
            _ => None,
        }
    }

    fn in_reply_to(&self) -> Option<usize> {
        if let MessageType::EchoOk { in_reply_to, .. } | MessageType::GenerateOk { in_reply_to, .. } | MessageType::BroadcastOk { in_reply_to, .. }= self {
            Some(*in_reply_to)
        } else {
            None
        }
    }

    fn broadcast_msg(&self) -> Option<usize> {
        if let MessageType::Broadcast { message, .. } = self {
            Some(*message)
        } else {
            None
        }
    }

    fn node_own_topology(&self) -> Option<&HashMap<String, Vec<String>>> {
        if let MessageType::Topology { topology, .. } = self {
            Some(topology)
        } else {
            None
        } 
    }

    fn delta(&self) -> Option<&String> {
        if let MessageType::VectorAdd { delta, .. } = self {
            Some(delta)
        } else {
            None
        } 
    }

    fn kv_key(&self) -> Option<&String> {
        if let MessageType::Send { key, .. } = self {
            Some(key)
        } else {
            None
        } 
    }

    fn kv_value(&self) -> Option<&String> {
        if let MessageType::Send { msg, .. } = self {
            Some(msg)
        } else {
            None
        } 
    }

    fn offsets(&self) -> Option<&HashMap<String, usize>> {
        if let MessageType::Poll { offsets, .. } = self {
            Some(offsets)
        } else {
            None
        }
    }

    fn keys(&self) -> Option<&Vec<String>> {
        if let MessageType::ListCommitedOffsets { keys, .. } = self {
            Some(keys)
        } else {
            None
        }
    }

    fn txn(&self) -> Option<&Vec<Vec<String>>> {
        if let MessageType::Transaction { txn, .. } = self {
            Some(txn)
        } else {
            None
        }
    }

    fn file_path(&self) -> Option<&String> {
        if let MessageType::ReadFromFile { file_path, .. } = self {
            Some(file_path)
        } else {
            None
        }
    }

    fn file_system_type(&self) -> Option<&String> {
        if let MessageType::ReadFromFile { accessibility, .. } = self {
            Some(accessibility)
        } else {
            None
        }
    }

    fn set_txn(&mut self, update_txn: Vec<Vec<String>>) {
        match self {
            MessageType::Transaction { ref mut txn, .. } => {
                *txn = update_txn
            }
            _ => {}
        }
    }
}

#[derive(Debug, Clone)]
pub enum MessageExceptions {
    PollOffsetsError,
    CommitOffsetsError,
    ListCommitedOffsetsError,
}


impl fmt::Display for MessageExceptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MessageExceptions::PollOffsetsError => write!(f, "Error polling offsets"),
            MessageExceptions::CommitOffsetsError => write!(f, "Error committing offsets"),
            MessageExceptions::ListCommitedOffsetsError => write!(f, "Error list committed offsets"),
        }
    }
}

impl Message {
    pub fn default_request_message(message_type: MessageType) -> Option<Self> {
        match message_type {
            MessageType::Transaction { .. } => Some(Message::Request { 
                src: "default".to_string(), 
                dest: "default".to_string(), 
                body: (
                    MessageType::Transaction { txn: vec![] }
                )
            }),
            _ => None,
        }
    }
}
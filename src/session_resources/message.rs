use serde::{Serialize, Deserialize};
use serde_json::Result;

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

// Impl generate message response on Message
impl Message {
    pub fn generate_response(&self, node_message_queue_ref: usize) -> Message {

    let request_body = self.body();

    match request_body {
        Some(message_type) => {

            match message_type {
                MessageType::Echo { .. } => {
                    Message::Response { src: (*self.dest().unwrap().clone()).to_string(), dest: (*self.src().unwrap().clone()).to_string()
                        , body: MessageType::EchoOk { msg_id: node_message_queue_ref + 1, in_reply_to: self.body().unwrap().msg_id(), echo: (*self.body().unwrap().echo().unwrap().clone()).to_string() } }
                },
                _ => {
                    panic!("No Response type exists for this Request!")
                }
            }
            
        }
        _ => panic!("Request type does not exist!")
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
}

// Trait to access fields in `MessageType`
pub trait MessageTypeFields {
    fn msg_id(&self) -> usize;
    fn echo(&self) -> Option<&String>;
    fn in_reply_to(&self) -> Option<usize>;
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
}

// Implement `MessageTypeFields` for `MessageType`
impl MessageTypeFields for MessageType {
    fn msg_id(&self) -> usize {
        match self {
            MessageType::Echo { msg_id, .. } | MessageType::EchoOk { msg_id, .. } => *msg_id,
        }
    }

    fn echo(&self) -> Option<&String> {
        match self {
            MessageType::Echo { echo, .. } | MessageType::EchoOk { echo, .. } => Some(echo),
            _ => None,
        }
    }

    fn in_reply_to(&self) -> Option<usize> {
        if let MessageType::EchoOk { in_reply_to, .. } = self {
            Some(*in_reply_to)
        } else {
            None
        }
    }
}
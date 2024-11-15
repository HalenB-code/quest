use serde_json::{Result, Value, Error};
use std::collections::{BTreeMap, HashMap, VecDeque};
use serde::{Serialize, Deserialize};
use serde::de::{self, Deserializer, MapAccess, Visitor};
use serde::{self, Serializer, ser::SerializeStruct};

// Message Handler Class
// Without deserializing an incoming message, the destination node which needs to produce the response will never be known
// Therefore a Message Handler is required to receive incoming requests -- these could be messages to STDIN or Session requests --
// and then route the message request to the correct node

// Deserializer
pub fn message_deserializer(input_string: String) -> Result<Message> {
    return serde_json::from_str(&input_string.trim());
}

// Serializer
pub fn message_serializer(output_string: &Message) -> Result<String> {
  return serde_json::to_string(output_string);
}

// Message Enum
#[derive(Debug, PartialEq, Eq)]
pub enum Message {
  Init {  msg_type: String, msg_id: usize, node_id: String, node_ids: Vec<String>},
  Request {  src: String, dest: String, body: MessageType },
  Response {  src: String, dest: String, body: MessageType },
}

// Impl generate message response on Message
impl Message {
    pub fn generate_response(self, node_message_queue_ref: usize) -> Message {

    match self.body { 
      MessageType::Echo => { 
        return Message::Response { src: self.dest, dest: self.src
          , body: EchoOk { typ: "echo_ok".to_string(), msg_id: node_message_queue_ref + 1, in_reply_to: self.body.msg_id, echo: self.body.echo} }
       },
      _ => panic!("No Response type exists for this Request")
    }
  }
}

// Message SubType: Init
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Init {
  #[serde(rename = "type")]
  pub msg_type: String,
  pub msg_id: usize,
  pub node_id: String,
  pub node_ids: Vec<String>
}

// Note on Request/Response structs
// These two structs are different sides of the same coin: for every request to a node there should be a response from a node
// Therefore an impl on Response should take in a Request and produce a Response; without the one there cannot be the other

// Message SubType: Request
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Request {
    pub src: String,
    pub dest: String,
    pub body: MessageType,
}

// Message SubType: Response
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Response {
    pub src: String,
    pub dest: String,
    pub body: MessageType,
}

// MessageType Class
// MessageType is the content of the Message
// Message is functional object of sending and receiving a message while MessageType determines the nature of what that message will say
#[derive(Debug, PartialEq, Eq)]
pub enum MessageType {
  Echo { typ: String, msg_id: usize, echo: String},
  EchoOk { typ: String, msg_id: usize, in_reply_to: usize, echo: String},
  // Broadcast,
  // Heartbeat,
}


#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Echo {
  #[serde(rename = "type")]
  pub typ: String,
  pub msg_id: usize,
  pub echo: String,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EchoOk {
  #[serde(rename = "type")]
  pub typ: String,
  pub msg_id: usize,
  pub in_reply_to: usize,
  pub echo: String,
}

// Implement custom deserialization for Message
// Note that the deserialization message type will only ever be Init or Request as these are messages coming in from STDIN encoded in JSON
impl<'de> Deserialize<'de> for Message {
  fn deserialize<D>(deserializer: D) -> Result<Self>
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
          // !todo("Add deserialization for MessageType to set enum on body");
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
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok>
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
    fn deserialize<D>(deserializer: D) -> Result<Self>
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
  fn serialize<S>(&self, serializer: S) -> Result<Self>
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

// Trait to access fields in `Message`
trait MessageFields {
    fn msg_type(&self) -> Option<&str>;
    fn msg_id(&self) -> Option<usize>;
    fn node_id(&self) -> Option<&str>;
    fn node_ids(&self) -> Option<&[String]>;
    fn src(&self) -> Option<&str>;
    fn dest(&self) -> Option<&str>;
    fn body(&self) -> Option<&dyn MessageTypeFields>;
}

// Trait to access fields in `MessageType`
trait MessageTypeFields {
    fn typ(&self) -> &str;
    fn msg_id(&self) -> usize;
    fn echo(&self) -> Option<&str>;
    fn in_reply_to(&self) -> Option<usize>;
}

// Implement `MessageFields` for `Message`
impl MessageFields for Message {
    fn msg_type(&self) -> Option<&str> {
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

    fn node_id(&self) -> Option<&str> {
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

    fn src(&self) -> Option<&str> {
        match self {
            Message::Request { src, .. } | Message::Response { src, .. } => Some(src),
            _ => None,
        }
    }

    fn dest(&self) -> Option<&str> {
        match self {
            Message::Request { dest, .. } | Message::Response { dest, .. } => Some(dest),
            _ => None,
        }
    }

    fn body(&self) -> Option<&dyn MessageTypeFields> {
        match self {
            Message::Request { body, .. } | Message::Response { body, .. } => Some(body),
            _ => None,
        }
    }
}

// Implement `MessageTypeFields` for `MessageType`
impl MessageTypeFields for MessageType {
    fn typ(&self) -> &str {
        match self {
            MessageType::Echo { typ, .. } | MessageType::EchoOk { typ, .. } => typ,
        }
    }

    fn msg_id(&self) -> usize {
        match self {
            MessageType::Echo { msg_id, .. } | MessageType::EchoOk { msg_id, .. } => *msg_id,
        }
    }

    fn echo(&self) -> Option<&str> {
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
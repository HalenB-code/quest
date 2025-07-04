use std::fmt;
use std::collections::HashMap;

use crate::session_resources::exceptions::ClusterExceptions;

#[derive(Debug, Clone)]
pub enum ClusterCommand {
    CmdMessageString {
        /*
            long = "-message",
            help = "Message to be sent to the cluster"
            default_value = ""
        */
        message: String,
    },
    CmdReadFile {
        /*
            long = "-fp",
            help = ""
            default_value = ""
        */
        target_file_path: String,
        /*
            long = "-target-node",
            help = "Single node reference or multiples nodes or all nodes: e.g. node-1; node-1,node-2; all"
            default_value = ""
        */
        target_node: String,
        /*
            long = "-delimiter",
            help = ""
            default_value = ""
        */
        delimiter: String,
    },
    CmdDisplayDf {
        /*
            long = "-df_name",
            help = ""
            default_value = ""
        */
        target_name: String,
        /*
            long = "-target-node",
            help = ""
            default_value = ""
        */
        target_node:String,
        /*
            long = "-n-rows",
            help = ""
            default_value = ""
        */
        n_rows: usize,
    },
    CmdGroupBy {
        /*
            long = "-df_name",
            help = ""
            default_value = ""
        */
        target_name: String,
        /*
            long = "-keys",
            help = ""
            default_value = ""
        */
        aggregation_keys: String,
        /*             
            long = "-aggregation",
            help = ""
            default_value = ""
        */
        aggregation_type: String,
    },
    CmdLogCluster {
        /*
            long = "-target",
            help = ""
            default_value = ""
        */
        logging_target: String,
    },
    CmdLogNode {
        /*
            long = "-target",
            help = ""
            default_value = ""
        */
        logging_target: String,
    }
}


impl fmt::Display for ClusterCommand {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ClusterCommand::CmdReadFile { .. } => write!(f, "read-file"),
            ClusterCommand::CmdDisplayDf { .. } => write!(f, "display-df"),
            ClusterCommand::CmdGroupBy { .. } => write!(f, "group-by"),
            ClusterCommand::CmdLogCluster { .. } => write!(f, "log-cluster"),
            ClusterCommand::CmdLogNode { .. } => write!(f, "log-node"),
            ClusterCommand::CmdMessageString { message } => write!(f, "message: {}", message),
        }
    }
}

impl ClusterCommand {

    pub fn new(action: &str) -> Result<ClusterCommand, ClusterExceptions> {

        match action {
            "read-file" => Ok(ClusterCommand::CmdReadFile {
                target_file_path: "".to_string(),
                target_node: "all".to_string(),
                delimiter: ",".to_string(),
            }),
            "display-df" => Ok(ClusterCommand::CmdDisplayDf {
                target_name: "".to_string(),
                target_node: "all".to_string(),
                n_rows: 5,
            }),
            "group-by" => Ok(ClusterCommand::CmdGroupBy {
                target_name: "".to_string(),
                aggregation_keys: "".to_string(),
                aggregation_type: "sum".to_string(),
            }),
            "log-cluster" => Ok(ClusterCommand::CmdLogCluster {
                logging_target: "stdout".to_string(),
            }),
            "log-node" => Ok(ClusterCommand::CmdLogNode {
                logging_target: "stdout".to_string(),
            }),
            _ if action.starts_with("message") => Ok(ClusterCommand::CmdMessageString {
                message: action.to_string(),
            }),
            _ if action.starts_with("{") => Ok(ClusterCommand::CmdMessageString {
                message: action.to_string(),
            }),
            _ => Err(ClusterExceptions::InvalidCommand { error_message: action.to_string() }),   
        }

    }

    pub fn map_command_line_request(args: String) -> Result<ClusterCommand, ClusterExceptions> {

        let arguments = args.split(" ").collect::<Vec<&str>>();
        let iter = &mut arguments.iter().peekable();
        let action: ClusterCommand;

        if let Some(action_parameter) = iter.next() {
            if let Ok(requested_action) = ClusterCommand::new(action_parameter) {
                action = requested_action;
            } else {
                return Err(ClusterExceptions::InvalidCommand { error_message: action_parameter.to_string() });
            }
        } else {
            return Err(ClusterExceptions::InvalidCommand { error_message: "No action provided".to_string() });
        }

        let mut remainig_args: HashMap<String, String> = HashMap::new();


        while let Some(arg) = iter.next() {
            remainig_args.insert(arg.to_string(), iter.next().unwrap_or(&"").to_string());
        };
        
        // CmdMessageString won't have an -action parameter so entire string is used as message and added to remainig_args with key -message
        if None == iter.next() {
            remainig_args.insert("-message".to_string(), args.to_string());
        };

        match action {
            ClusterCommand::CmdMessageString { .. } => {
                let mapped_action = ClusterCommand::CmdMessageString {
                    message: remainig_args.get("-message").unwrap_or(&"".to_string()).to_string(),
                };

                Ok(mapped_action.clone())
            },
            ClusterCommand::CmdReadFile { .. } => {
                let mapped_action = ClusterCommand::CmdReadFile {
                    target_file_path: remainig_args.get("-fp").unwrap_or(&"".to_string()).to_string(),
                    target_node: remainig_args.get("-target-node").unwrap_or(&"".to_string()).to_string(),
                    delimiter: remainig_args.get("-delimiter").unwrap_or(&"".to_string()).to_string(),
                };

                Ok(mapped_action)
            },
            ClusterCommand::CmdDisplayDf { .. } => {
                let mapped_action = ClusterCommand::CmdDisplayDf {
                    target_name: remainig_args.get("-df_name").unwrap_or(&"".to_string()).to_string(),
                    target_node: remainig_args.get("-target-node").unwrap_or(&"".to_string()).to_string(),
                    n_rows: remainig_args.get("-n-rows").unwrap().parse::<usize>().unwrap_or(5_usize),
                };
                
                Ok(mapped_action)
            },
            ClusterCommand::CmdGroupBy { .. } => {
                let mapped_action = ClusterCommand::CmdGroupBy {
                    target_name: remainig_args.get("-df_name").unwrap_or(&"".to_string()).to_string(),
                    aggregation_keys: remainig_args.get("-keys").unwrap_or(&"".to_string()).to_string(),
                    aggregation_type: remainig_args.get("-aggregation").unwrap_or(&"".to_string()).to_string(),
                };

                Ok(mapped_action)
            },
            ClusterCommand::CmdLogCluster { .. } => {
                let mapped_action = ClusterCommand::CmdLogCluster {
                    logging_target: remainig_args.get("-target").unwrap_or(&"".to_string()).to_string(),
                };

                Ok(mapped_action)
            },
            ClusterCommand::CmdLogNode { .. } => {
                let mapped_action = ClusterCommand::CmdLogNode {
                    logging_target: remainig_args.get("-target").unwrap_or(&"".to_string()).to_string(),
                };

                Ok(mapped_action)
            },
        }
    }
}


#[derive(Debug, Clone)]
pub enum CliTypeExceptions {
    InvalidCommand { error_message: String },
    InvalidArgument { error_message: String },
    FailedToMapCommand { error_message: String },
}


impl fmt::Display for CliTypeExceptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CliTypeExceptions::InvalidCommand { error_message} => write!(f, "Invalid command provided '{}'.", error_message),
            CliTypeExceptions::InvalidArgument { error_message} => write!(f, "Invalid argument provided '{}'.", error_message),
            CliTypeExceptions::FailedToMapCommand { error_message} => write!(f, "Failed to map command to a known command type '{}'.", error_message),
        }
    }
}
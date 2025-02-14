use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use serde::{Serialize, Deserialize};

use std::io::BufRead;
use crate::session_resources::transactions::Action;

#[derive(Serialize, Deserialize, Debug)]
pub enum WalEntry {
    TransactionStart { transaction_id: usize },
    ActionLog { transaction_id: usize, node: String, action: Action },
    TransactionCommit { transaction_id: usize },
    TransactionAbort { transaction_id: usize },
}

#[derive(Debug)]
pub struct WriteAheadLog {
    pub file: BufWriter<File>,
}

impl WriteAheadLog {
    pub fn new(path: &str) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        Ok(Self {
            file: BufWriter::new(file),
        })
    }

    pub fn write_entry(&mut self, entry: &WalEntry) -> io::Result<()> {
        let serialized = serde_json::to_string(entry)?;
        writeln!(self.file, "{}", serialized)?;
        self.file.flush()?;
        Ok(())
    }
}

impl WriteAheadLog {
    pub fn replay(path: &str) -> io::Result<Vec<WalEntry>> {
        let file = File::open(path)?;
        let reader = io::BufReader::new(file);
        let mut entries = Vec::new();
        for line in reader.lines() {
            let entry: WalEntry = serde_json::from_str(&line?)?;
            entries.push(entry);
        }
        Ok(entries)
    }
}

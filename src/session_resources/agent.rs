use std::io::{self, BufRead, Write};
use std::net::TcpStream;
use std::thread;

fn main() -> std::io::Result<()> {
    // Connect to the SSH-forwarded local port (e.g., 9000)
    let mut stream = TcpStream::connect("127.0.0.1:9000")?;
    println!("Connected to server via SSH tunnel!");

    let mut stream_clone = stream.try_clone().unwrap();

    // Spawn a thread to handle incoming messages from the server
    thread::spawn(move || {
        let reader = io::BufReader::new(stream_clone);
        for line in reader.lines() {
            match line {
                Ok(msg) => {
                    println!("Server says: {}", msg);
                }
                Err(err) => {
                    eprintln!("Error reading from server: {}", err);
                    break;
                }
            }
        }
    });

    // Send messages to the server from STDIN
    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        let msg = line.unwrap();
        if msg == "exit" {
            println!("Exiting...");
            break;
        }
        stream.write_all(msg.as_bytes())?;
        stream.write_all(b"\n")?;
    }

    Ok(())
}


trait Agentic {
    pub fn create();
    pub fn remove();
    pub fn coordinator_response();
    pub fn coordinator_inform();
    pub fn agent_response();
    pub fn agent_inform();
};
use std::net::{TcpListener, TcpStream};
use std::io::{Read, Write, BufReader, BufRead};
use std::sync::Arc;
use std::thread;

fn handle_client(mut stream: TcpStream) {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut buffer = String::new();

    loop {
        buffer.clear();
        // Read client message
        match reader.read_line(&mut buffer) {
            Ok(0) => {
                println!("Client disconnected.");
                break;
            },
            Ok(_) => {
                // Print message from client
                println!("Client says: {}", buffer.trim_end());
                
                // Echo message back to client
                stream.write_all(buffer.as_bytes()).unwrap();
            },
            Err(err) => {
                eprintln!("Error reading from client: {}", err);
                break;
            }
        }
    }
}

fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:7878")?;
    println!("Server listening on port 7878");

    let listener = Arc::new(listener);

    // Accept clients
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("New client connected!");

                // Spawn a new thread for each client
                thread::spawn(move || {
                    handle_client(stream);
                });
            }
            Err(e) => {
                eprintln!("Error accepting connection: {}", e);
            }
        }
    }
    Ok(())
}

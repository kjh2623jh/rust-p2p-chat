use std::{
    collections::HashMap,
    io::{self, BufRead, BufReader},
    net::{SocketAddr, TcpListener, TcpStream},
};

fn main() -> io::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:9000")?;

    println!("Signaling server listening on 0.0.0.0:9000");

    let mut rooms: HashMap<String, Vec<SocketAddr>> = HashMap::new();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                handle_client(stream, &mut rooms)?;
            }

            Err(error) => {
                eprintln!("Connection error: {error}");
            }
        }
    }

    Ok(())
}

fn handle_client(
    stream: TcpStream,
    rooms: &mut HashMap<String, Vec<SocketAddr>>,
) -> io::Result<()> {
    let client_addr = stream.peer_addr()?;

    println!("Client connected: {client_addr}");

    let mut reader = BufReader::new(stream);

    let mut message = String::new();

    reader.read_line(&mut message)?;

    let message = message.trim();

    let mut parts = message.split_whitespace();

    match (parts.next(), parts.next()) {
        (Some("JOIN"), Some(room_code)) => {
            let room = rooms.entry(room_code.to_string()).or_default();

            room.push(client_addr);

            println!("{client_addr} joined room {room_code}");

            println!("Current rooms:");
            println!("{rooms:#?}");
        }

        _ => {
            println!("Unknown message from {client_addr}: {message}");
        }
    }

    Ok(())
}

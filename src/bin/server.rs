use std::{collections::HashMap, sync::Arc};

use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

type Rooms = Arc<Mutex<HashMap<String, Vec<String>>>>;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:9000").await?;

    let rooms: Rooms = Arc::new(Mutex::new(HashMap::new()));

    println!("Signaling server listening on 0.0.0.0:9000");

    loop {
        let (stream, addr) = listener.accept().await?;

        println!("Client connected: {addr}");

        let rooms = Arc::clone(&rooms);

        tokio::spawn(async move {
            if let Err(error) = handle_client(stream, rooms).await {
                eprintln!("Client error ({addr}): {error}");
            }
        });
    }
}

async fn handle_client(stream: TcpStream, rooms: Rooms) -> std::io::Result<()> {
    let addr = stream.peer_addr()?;

    let mut reader = BufReader::new(stream);

    let mut message = String::new();

    reader.read_line(&mut message).await?;

    let message = message.trim();

    let mut parts = message.split_whitespace();

    match (parts.next(), parts.next()) {
        (Some("JOIN"), Some(room_code)) => {
            let mut rooms = rooms.lock().await;

            let room = rooms.entry(room_code.to_string()).or_default();

            room.push(addr.to_string());

            println!("{addr} joined room {room_code}");

            println!("Rooms: {rooms:#?}");
        }

        _ => {
            println!("Unknown message: {message}");
        }
    }

    Ok(())
}

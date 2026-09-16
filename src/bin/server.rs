use std::{collections::HashMap, sync::Arc};

use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream, tcp::OwnedWriteHalf},
    sync::{Mutex, mpsc},
};

type ClientSender = mpsc::UnboundedSender<String>;

type Rooms = Arc<Mutex<HashMap<String, HashMap<String, ClientSender>>>>;

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
    let client_id = addr.to_string();

    let (reader, writer) = stream.into_split();

    let mut reader = BufReader::new(reader);

    let (tx, rx) = mpsc::unbounded_channel::<String>();

    tokio::spawn(write_messages(writer, rx));

    let mut current_room: Option<String> = None;

    loop {
        let mut message = String::new();

        let bytes_read = reader.read_line(&mut message).await?;

        if bytes_read == 0 {
            println!("Client disconnected: {client_id}");
            break;
        }

        let message = message.trim();

        let mut parts = message.split_whitespace();

        match parts.next() {
            Some("JOIN") => {
                if let Some(room_code) = parts.next() {
                    join_room(&rooms, room_code, &client_id, tx.clone()).await;

                    current_room = Some(room_code.to_string());
                }
            }

            Some("MSG") => {
                let text = parts.collect::<Vec<_>>().join(" ");

                if let Some(room_code) = &current_room {
                    broadcast(&rooms, room_code, &client_id, &text).await;
                }
            }

            _ => {
                let _ = tx.send("ERROR unknown command\n".to_string());
            }
        }
    }

    if let Some(room_code) = current_room {
        leave_room(&rooms, &room_code, &client_id).await;
    }

    Ok(())
}

async fn join_room(rooms: &Rooms, room_code: &str, client_id: &str, sender: ClientSender) {
    let mut rooms = rooms.lock().await;

    let room = rooms.entry(room_code.to_string()).or_default();

    for peer_sender in room.values() {
        let _ = peer_sender.send(format!("PEER_JOINED {client_id}\n"));
    }

    room.insert(client_id.to_string(), sender.clone());

    let _ = sender.send(format!("JOINED {room_code}\n"));

    println!("{client_id} joined {room_code}");
}

async fn broadcast(rooms: &Rooms, room_code: &str, sender_id: &str, message: &str) {
    let rooms = rooms.lock().await;

    let Some(room) = rooms.get(room_code) else {
        return;
    };

    for (client_id, sender) in room {
        if client_id == sender_id {
            continue;
        }

        let _ = sender.send(format!("MESSAGE {sender_id} {message}\n"));
    }
}

async fn leave_room(rooms: &Rooms, room_code: &str, client_id: &str) {
    let mut rooms = rooms.lock().await;

    let should_remove_room = if let Some(room) = rooms.get_mut(room_code) {
        room.remove(client_id);

        for sender in room.values() {
            let _ = sender.send(format!("PEER_LEFT {client_id}\n"));
        }

        room.is_empty()
    } else {
        false
    };

    if should_remove_room {
        rooms.remove(room_code);
    }

    println!("{client_id} left {room_code}");
}

async fn write_messages(mut writer: OwnedWriteHalf, mut receiver: mpsc::UnboundedReceiver<String>) {
    while let Some(message) = receiver.recv().await {
        if writer.write_all(message.as_bytes()).await.is_err() {
            break;
        }
    }
}

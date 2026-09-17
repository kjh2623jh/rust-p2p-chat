use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream, tcp::OwnedWriteHalf},
    sync::{Mutex, mpsc},
};

type ClientSender = mpsc::Sender<String>;

#[derive(Clone)]
struct Peer {
    sender: ClientSender,
    udp_addr: Option<SocketAddr>,
}

type Rooms = Arc<Mutex<HashMap<String, HashMap<String, Peer>>>>;

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
    let (tx, rx) = mpsc::channel::<String>(32);

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
            Some("REGISTER_UDP") => {
                let Some(port) = parts.next() else {
                    continue;
                };
                let Ok(port) = port.parse::<u16>() else {
                    continue;
                };
                let Some(room_code) = &current_room else {
                    continue;
                };
                let udp_addr = SocketAddr::new(addr.ip(), port);

                register_udp(&rooms, room_code, &client_id, udp_addr).await;
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

    room.insert(
        client_id.to_string(),
        Peer {
            sender: sender.clone(),
            udp_addr: None,
        },
    );

    let _ = sender.send(format!("JOINED {room_code}\n")).await;
    println!("{client_id} joined {room_code}");
}

async fn broadcast(rooms: &Rooms, room_code: &str, sender_id: &str, message: &str) {
    let rooms = rooms.lock().await;
    let Some(room) = rooms.get(room_code) else {
        return;
    };

    for (client_id, peer) in room {
        if client_id == sender_id {
            continue;
        }

        let _ = peer
            .sender
            .send(format!("MESSAGE {sender_id} {message}\n"))
            .await;
    }
}

async fn leave_room(rooms: &Rooms, room_code: &str, client_id: &str) {
    let mut rooms = rooms.lock().await;
    let should_remove_room = if let Some(room) = rooms.get_mut(room_code) {
        room.remove(client_id);

        for peer in room.values() {
            let _ = peer.sender.send(format!("PEER_LEFT {client_id}\n")).await;
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

async fn write_messages(mut writer: OwnedWriteHalf, mut receiver: mpsc::Receiver<String>) {
    while let Some(message) = receiver.recv().await {
        if writer.write_all(message.as_bytes()).await.is_err() {
            break;
        }
    }
}

async fn register_udp(rooms: &Rooms, room_code: &str, client_id: &str, udp_addr: SocketAddr) {
    let mut rooms = rooms.lock().await;
    let Some(room) = rooms.get_mut(room_code) else {
        return;
    };
    let Some(peer) = room.get_mut(client_id) else {
        return;
    };

    peer.udp_addr = Some(udp_addr);

    println!("{client_id} registered UDP: {udp_addr}");

    let ready_peers: Vec<(String, SocketAddr, ClientSender)> = room
        .iter()
        .filter_map(|(id, peer)| {
            peer.udp_addr
                .map(|addr| (id.clone(), addr, peer.sender.clone()))
        })
        .collect();

    if ready_peers.len() == 2 {
        let (_, addr_a, sender_a) = &ready_peers[0];
        let (_, addr_b, sender_b) = &ready_peers[1];

        let _ = sender_a.send(format!("PEER {addr_b}\n")).await;
        let _ = sender_b.send(format!("PEER {addr_a}\n")).await;
    }
}

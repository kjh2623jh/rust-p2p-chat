use std::{collections::HashMap, io, net::SocketAddr, sync::Arc};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream, UdpSocket, tcp::OwnedWriteHalf},
    sync::{Mutex, mpsc},
};

type ClientSender = mpsc::Sender<String>;

#[derive(Clone)]
struct Peer {
    sender: ClientSender,
    udp_addr: Option<SocketAddr>,
}

type Room = HashMap<String, Peer>;
type Rooms = Arc<Mutex<HashMap<String, Room>>>;

#[tokio::main]
async fn main() -> io::Result<()> {
    let tcp_listener = TcpListener::bind("0.0.0.0:9000").await?;
    let udp_socket = UdpSocket::bind("0.0.0.0:9001").await?;
    let rooms: Rooms = Arc::new(Mutex::new(HashMap::new()));

    println!("TCP signaling : 0.0.0.0:9000");
    println!("UDP discovery : 0.0.0.0:9001");

    // UDP endpoint 등록 처리
    {
        let rooms = Arc::clone(&rooms);

        tokio::spawn(async move {
            if let Err(error) = handle_udp_registration(udp_socket, rooms).await {
                eprintln!("UDP registration error: {error}");
            }
        });
    }

    // TCP signaling
    loop {
        let (stream, addr) = tcp_listener.accept().await?;

        println!("TCP connected: {addr}");

        let rooms = Arc::clone(&rooms);

        tokio::spawn(async move {
            if let Err(error) = handle_client(stream, rooms).await {
                eprintln!("Client error ({addr}): {error}");
            }
        });
    }
}

async fn handle_client(stream: TcpStream, rooms: Rooms) -> io::Result<()> {
    let tcp_addr = stream.peer_addr()?;
    // 지금은 TCP endpoint 자체를 임시 client_id로 사용
    let client_id = tcp_addr.to_string();
    let (reader, writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let (tx, rx) = mpsc::channel::<String>(32);

    tokio::spawn(write_messages(writer, rx));

    // client에게 자신의 ID 전달
    if tx.send(format!("CLIENT_ID {client_id}\n")).await.is_err() {
        return Ok(());
    }

    let mut current_room: Option<String> = None;

    loop {
        let mut message = String::new();

        let bytes_read = match reader.read_line(&mut message).await {
            Ok(size) => size,
            Err(error) => {
                eprintln!("Client connection error ({client_id}): {error}");
                break;
            }
        };

        if bytes_read == 0 {
            println!("Client disconnected: {client_id}");
            break;
        }

        let message = message.trim();
        let mut parts = message.split_whitespace();

        match parts.next() {
            Some("JOIN") => {
                // 이미 다른 방에 들어가 있다면 거절
                // if current_room.is_some() {
                //     let _ = tx.send("ALREADY_IN_ROOM\n".to_string()).await;
                //     continue;
                // }

                let Some(room_code) = parts.next() else {
                    let _ = tx.send("ERROR INVALID_JOIN\n".to_string()).await;
                    continue;
                };

                // 방에 참가
                match join_room(&rooms, room_code, &client_id, tx.clone()).await {
                    Ok(()) => {
                        current_room = Some(room_code.to_string());
                    }
                    Err(error) => {
                        let _ = tx.send(format!("{error}\n")).await;
                    }
                }
            }

            Some("LEAVE") => {
                let Some(room_code) = current_room.take() else {
                    let _ = tx.send("NOT_IN_ROOM\n".to_string()).await;
                    continue;
                };

                leave_room(&rooms, &room_code, &client_id).await;

                let _ = tx.send("LEFT\n".to_string()).await;
            }

            _ => {
                let _ = tx.send("ERROR UNKNOWN_COMMAND\n".to_string()).await;
            }
        }
    }

    // 연결이 끊겼다면 room에서 제거
    if let Some(room_code) = current_room {
        leave_room(&rooms, &room_code, &client_id).await;
    }

    Ok(())
}

async fn join_room(
    rooms: &Rooms,
    room_code: &str,
    client_id: &str,
    sender: ClientSender,
) -> Result<(), &'static str> {
    let mut rooms = rooms.lock().await;
    let room = rooms.entry(room_code.to_string()).or_default();

    // 같은 client_id 중복 방어
    if room.contains_key(client_id) {
        return Err("ALREADY_JOINED");
    }

    // 1:1 P2P이므로 최대 2명
    if room.len() >= 2 {
        return Err("ROOM_FULL");
    }

    room.insert(
        client_id.to_string(),
        Peer {
            sender: sender.clone(),
            udp_addr: None,
        },
    );

    println!("{client_id} joined room {room_code}");

    drop(rooms);

    let _ = sender.send(format!("JOINED {room_code}\n")).await;

    Ok(())
}

async fn leave_room(rooms: &Rooms, room_code: &str, client_id: &str) {
    // lock 잡은 상태에서 await 하지 않기 위해
    // sender들만 먼저 복사해둔다.
    let remaining_senders = {
        let mut rooms = rooms.lock().await;
        let mut senders = Vec::new();
        let mut remove_room = false;

        if let Some(room) = rooms.get_mut(room_code) {
            room.remove(client_id);
            senders = room.values().map(|peer| peer.sender.clone()).collect();
            remove_room = room.is_empty();
        }

        if remove_room {
            rooms.remove(room_code);
        }

        senders
    };

    for sender in remaining_senders {
        let _ = sender.send(format!("PEER_LEFT {client_id}\n")).await;
    }

    println!("{client_id} left room {room_code}");
}

async fn write_messages(mut writer: OwnedWriteHalf, mut receiver: mpsc::Receiver<String>) {
    while let Some(message) = receiver.recv().await {
        if writer.write_all(message.as_bytes()).await.is_err() {
            break;
        }
    }
}

async fn handle_udp_registration(socket: UdpSocket, rooms: Rooms) -> io::Result<()> {
    let mut buffer = [0u8; 1024];

    loop {
        let (size, source_addr) = socket.recv_from(&mut buffer).await?;
        let message = String::from_utf8_lossy(&buffer[..size]);
        let message = message.trim();
        let mut parts = message.split_whitespace();

        match parts.next() {
            Some("REGISTER") => {
                let Some(client_id) = parts.next() else {
                    continue;
                };

                println!(
                    "UDP REGISTER: \
                     {client_id} -> \
                     {source_addr}"
                );

                register_udp_addr(&rooms, client_id, source_addr).await;
            }
            _ => {
                println!(
                    "Unknown UDP packet \
                     from {source_addr}: \
                     {message}"
                );
            }
        }
    }
}

async fn register_udp_addr(rooms: &Rooms, client_id: &str, udp_addr: SocketAddr) {
    // 여기서는 lock 안에서 room만 수정하고
    // 실제 tx.send().await는 lock 해제 후 수행한다.
    let peers_to_notify = {
        let mut rooms = rooms.lock().await;
        let mut result: Option<Vec<(ClientSender, SocketAddr)>> = None;

        for (room_code, room) in rooms.iter_mut() {
            let Some(peer) = room.get_mut(client_id) else {
                continue;
            };

            peer.udp_addr = Some(udp_addr);

            println!(
                "{client_id} UDP registered \
                 in room {room_code}: \
                 {udp_addr}"
            );

            // 방에 정확히 2명이 있고
            // 둘 다 UDP 주소가 준비되어야 함
            if room.len() == 2 {
                let ready: Vec<(ClientSender, SocketAddr)> = room
                    .values()
                    .filter_map(|peer| peer.udp_addr.map(|addr| (peer.sender.clone(), addr)))
                    .collect();

                if ready.len() == 2 {
                    result = Some(ready);
                }
            }

            break;
        }

        result
    };

    let Some(peers) = peers_to_notify else {
        return;
    };

    let (sender_a, addr_a) = &peers[0];
    let (sender_b, addr_b) = &peers[1];

    println!(
        "P2P candidates:\n\
         A = {addr_a}\n\
         B = {addr_b}"
    );

    // A에게 B 주소
    let _ = sender_a.send(format!("PEER {addr_b}\n")).await;
    // B에게 A 주소
    let _ = sender_b.send(format!("PEER {addr_a}\n")).await;
}

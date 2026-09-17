use std::{net::SocketAddr, sync::Arc};

use tokio::{
    io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpStream, UdpSocket},
    sync::Mutex,
};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let stream = TcpStream::connect("127.0.0.1:9000").await?;
    let udp_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await?);
    println!("UDP socket: {}", udp_socket.local_addr()?);

    let peer_addr = Arc::new(Mutex::new(None::<SocketAddr>));
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    writer.write_all(b"JOIN ABC123\n").await?;

    let udp_port = udp_socket.local_addr()?.port();

    writer
        .write_all(format!("REGISTER_UDP {udp_port}\n").as_bytes())
        .await?;

    let peer_addr_clone = Arc::clone(&peer_addr);

    tokio::spawn(async move {
        loop {
            let mut message = String::new();

            match reader.read_line(&mut message).await {
                Ok(0) => break,
                Ok(_) => {
                    let message = message.trim();

                    println!("[SERVER] {message}");

                    let mut parts = message.split_whitespace();

                    if parts.next() == Some("PEER") {
                        if let Some(addr) = parts.next() {
                            if let Ok(addr) = addr.parse::<SocketAddr>() {
                                *peer_addr_clone.lock().await = Some(addr);

                                println!("Peer discovered: {addr}");
                            }
                        }
                    }
                }
                Err(error) => {
                    eprintln!("{error}");
                    break;
                }
            }
        }
    });

    let udp_receiver = Arc::clone(&udp_socket);

    tokio::spawn(async move {
        let mut buffer = [0u8; 1024];

        loop {
            match udp_receiver.recv_from(&mut buffer).await {
                Ok((size, from)) => {
                    let message = String::from_utf8_lossy(&buffer[..size]);

                    println!("\n[P2P {from}] {message}");
                }

                Err(error) => {
                    eprintln!("UDP receive error: {error}");
                    break;
                }
            }
        }
    });

    let stdin = BufReader::new(io::stdin());
    let mut lines = stdin.lines();

    while let Some(line) = lines.next_line().await? {
        let peer = *peer_addr.lock().await;
        let Some(peer) = peer else {
            println!("아직 Peer가 없습니다.");
            continue;
        };

        udp_socket.send_to(line.as_bytes(), peer).await?;
    }

    Ok(())
}

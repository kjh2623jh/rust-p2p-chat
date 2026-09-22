use std::{io, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{self as tokio_io, AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpStream, UdpSocket},
    sync::RwLock,
    time::sleep,
};

const TCP_SERVER: &str = "127.0.0.1:9000";
const UDP_SERVER: &str = "127.0.0.1:9001";
const ROOM_CODE: &str = "ABC123";

#[tokio::main]
async fn main() -> io::Result<()> {
    println!("Connecting to signaling server...");

    let tcp_stream = TcpStream::connect(TCP_SERVER).await?;

    println!("TCP signaling connected");

    /*
     * P2P용 UDP socket.
     *
     * 이 하나의 socket을
     * 1. signaling UDP 등록
     * 2. hole punching
     * 3. 실제 채팅
     *
     * 모두에 사용해야 한다.
     */
    let udp_socket = Arc::new(UdpSocket::bind("0.0.0.0:0").await?);
    println!("Local UDP socket: {}", udp_socket.local_addr()?);
    let peer_addr: Arc<RwLock<Option<SocketAddr>>> = Arc::new(RwLock::new(None));
    let (reader, mut writer) = tcp_stream.into_split();
    let mut reader = BufReader::new(reader);

    /*
     * 1.
     * 서버가 CLIENT_ID를 먼저 보내줌.
     */
    let client_id = read_client_id(&mut reader).await?;

    println!("Client ID: {client_id}");

    /*
     * 2.
     * Room 입장
     */
    writer
        .write_all(format!("JOIN {ROOM_CODE}\n").as_bytes())
        .await?;

    /*
     * UDP REGISTER를 바로 보내지 않는다.
     *
     * TCP와 UDP는 서로 다른 전송 경로라
     * UDP REGISTER가 JOIN보다 먼저
     * 서버에 도착할 수도 있기 때문이다.
     *
     * 반드시 JOINED를 받은 뒤 진행.
     */
    let joined = wait_for_join_result(&mut reader).await?;

    if !joined {
        return Ok(());
    }

    println!("Joined room: {ROOM_CODE}");

    /*
     * 3.
     * 같은 UDP socket으로
     * signaling server에 REGISTER 전송.
     *
     * 서버의 recv_from()이
     * 실제 외부 UDP endpoint를 관측한다.
     */
    let register_message = format!("REGISTER {client_id}");

    udp_socket
        .send_to(register_message.as_bytes(), UDP_SERVER)
        .await?;

    println!("UDP endpoint registration sent");

    /*
     * TCP signaling 수신 task
     */
    {
        let peer_addr = Arc::clone(&peer_addr);
        let udp_socket = Arc::clone(&udp_socket);

        tokio::spawn(async move {
            signaling_receive_loop(reader, peer_addr, udp_socket).await;
        });
    }

    /*
     * UDP 수신 task
     */
    {
        let udp_socket = Arc::clone(&udp_socket);
        let peer_addr = Arc::clone(&peer_addr);

        tokio::spawn(async move {
            udp_receive_loop(udp_socket, peer_addr).await;
        });
    }

    /*
     * 사용자 입력 -> UDP P2P 전송
     */
    let stdin = tokio_io::stdin();
    let mut lines = BufReader::new(stdin).lines();

    println!();
    println!("메시지를 입력하세요.");

    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }

        let peer = *peer_addr.read().await;

        let Some(peer) = peer else {
            println!("아직 P2P 연결 상대가 없습니다.");
            continue;
        };

        udp_socket.send_to(line.as_bytes(), peer).await?;

        println!("[ME] {line}");
    }

    Ok(())
}

async fn read_client_id<R>(reader: &mut R) -> io::Result<String>
where
    R: AsyncBufReadExt + Unpin,
{
    let mut line = String::new();
    let size = reader.read_line(&mut line).await?;

    if size == 0 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "server disconnected",
        ));
    }

    let mut parts = line.trim().split_whitespace();

    match (parts.next(), parts.next()) {
        (Some("CLIENT_ID"), Some(id)) => Ok(id.to_string()),

        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid CLIENT_ID response",
        )),
    }
}

async fn wait_for_join_result<R>(reader: &mut R) -> io::Result<bool>
where
    R: AsyncBufReadExt + Unpin,
{
    loop {
        let mut line = String::new();
        let size = reader.read_line(&mut line).await?;

        if size == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "server disconnected",
            ));
        }

        let message = line.trim();

        match message {
            value if value.starts_with("JOINED ") => {
                return Ok(true);
            }

            "ROOM_FULL" => {
                println!("방이 가득 찼습니다.");
                return Ok(false);
            }

            "ALREADY_IN_ROOM" | "ALREADY_JOINED" => {
                println!("이미 방에 들어가 있습니다.");
                return Ok(false);
            }

            _ => {
                println!("[SERVER] {message}");
            }
        }
    }
}

async fn signaling_receive_loop<R>(
    mut reader: R,
    peer_addr: Arc<RwLock<Option<SocketAddr>>>,
    udp_socket: Arc<UdpSocket>,
) where
    R: AsyncBufReadExt + Unpin,
{
    loop {
        let mut line = String::new();

        match reader.read_line(&mut line).await {
            Ok(0) => {
                println!("Signaling server disconnected");
                break;
            }

            Ok(_) => {
                let message = line.trim();
                println!("[SERVER] {message}");

                let mut parts = message.split_whitespace();

                match parts.next() {
                    Some("PEER") => {
                        let Some(addr) = parts.next() else {
                            continue;
                        };

                        let Ok(addr) = addr.parse::<SocketAddr>() else {
                            continue;
                        };

                        {
                            let mut peer = peer_addr.write().await;

                            *peer = Some(addr);
                        }

                        println!("Peer endpoint: {addr}");

                        /*
                         * A/B 둘 다 이 작업을 실행하면서
                         * 서로 동시에 UDP를 보내 NAT hole을 연다.
                         */
                        start_hole_punching(Arc::clone(&udp_socket), addr);
                    }

                    Some("PEER_LEFT") => {
                        let mut peer = peer_addr.write().await;

                        *peer = None;

                        println!("Peer disconnected");
                    }

                    _ => {}
                }
            }

            Err(error) => {
                eprintln!("Signaling receive error: {error}");
                break;
            }
        }
    }
}

fn start_hole_punching(udp_socket: Arc<UdpSocket>, peer_addr: SocketAddr) {
    tokio::spawn(async move {
        println!("Starting hole punching...");

        /*
         * 첫 UDP 패킷은 NAT 상태 때문에
         * 버려질 수 있어서 여러 번 보낸다.
         */
        for attempt in 1..=10 {
            match udp_socket.send_to(b"PUNCH", peer_addr).await {
                Ok(_) => {
                    println!("PUNCH #{attempt} -> {peer_addr}");
                }

                Err(error) => {
                    eprintln!("Punch send error: {error}");
                }
            }

            sleep(Duration::from_millis(250)).await;
        }
    });
}

async fn udp_receive_loop(udp_socket: Arc<UdpSocket>, peer_addr: Arc<RwLock<Option<SocketAddr>>>) {
    let mut buffer = [0u8; 2048];

    loop {
        let (size, from) = match udp_socket.recv_from(&mut buffer).await {
            Ok(result) => result,

            Err(error) => {
                eprintln!("UDP receive error: {error}");
                break;
            }
        };

        let message = String::from_utf8_lossy(&buffer[..size]);

        match message.as_ref() {
            "PUNCH" => {
                /*
                 * 실제로 패킷이 도착한 주소를
                 * peer 주소로 사용.
                 */
                {
                    let mut peer = peer_addr.write().await;

                    *peer = Some(from);
                }

                println!("PUNCH received from {from}");

                let _ = udp_socket.send_to(b"PUNCH_ACK", from).await;
            }

            "PUNCH_ACK" => {
                {
                    let mut peer = peer_addr.write().await;

                    *peer = Some(from);
                }

                println!("P2P connected: {from}");
            }

            _ => {
                println!("[P2P {from}] {message}");
            }
        }
    }
}

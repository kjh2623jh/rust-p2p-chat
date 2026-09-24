use super::{NetworkCommand, NetworkEvent};
use std::{
    io,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpStream, UdpSocket, lookup_host},
    sync::{RwLock, mpsc},
    time::sleep,
};

const TCP_SERVER: &str = "p2psignal.mcv.kr:9000";
const UDP_SERVER: &str = "p2psignal.mcv.kr:9001";

#[derive(Clone)]
struct P2pState {
    udp_socket: Arc<UdpSocket>,
    peer_addr: Arc<RwLock<Option<SocketAddr>>>,
    connected: Arc<AtomicBool>,
    session_id: Arc<AtomicU64>,
}
impl P2pState {
    async fn set_peer(&self, addr: Option<SocketAddr>) {
        let mut peer = self.peer_addr.write().await;
        *peer = addr;
    }

    async fn get_peer(&self) -> Option<SocketAddr> {
        *self.peer_addr.read().await
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }

    fn set_connected(&self, value: bool) {
        self.connected.store(value, Ordering::Relaxed);
    }

    fn start_session(&self) -> u64 {
        self.session_id.fetch_add(1, Ordering::Relaxed) + 1
    }

    fn is_current_session(&self, session_id: u64) -> bool {
        self.session_id.load(Ordering::Relaxed) == session_id
    }

    async fn reset(&self) {
        self.start_session();
        self.set_peer(None).await;
        self.set_connected(false);
    }

    async fn mark_connected_if_peer(
        &self,
        addr: SocketAddr,
        event_tx: &mpsc::Sender<NetworkEvent>,
    ) -> bool {
        let Ok(event_permit) = event_tx.reserve().await else {
            return false;
        };
        let peer = self.peer_addr.read().await;

        if *peer != Some(addr) || self.connected.swap(true, Ordering::Relaxed) {
            return false;
        }

        // peer read lock을 유지한 채 enqueue하여 reset 이후에
        // 오래된 PeerConnected 이벤트가 전달되는 순서 역전을 막는다.
        event_permit.send(NetworkEvent::PeerConnected);
        true
    }
}

pub struct NetworkClient {
    command_rx: mpsc::Receiver<NetworkCommand>,
    event_tx: mpsc::Sender<NetworkEvent>,
}
impl NetworkClient {
    pub fn new(
        command_rx: mpsc::Receiver<NetworkCommand>,
        event_tx: mpsc::Sender<NetworkEvent>,
    ) -> Self {
        Self {
            command_rx,
            event_tx,
        }
    }

    pub async fn run(mut self) {
        if let Err(error) = self.run_inner().await {
            eprintln!("Network error: {error}");

            let _ = self
                .event_tx
                .send(NetworkEvent::Error(error.to_string()))
                .await;
        }

        let _ = self.event_tx.send(NetworkEvent::ServerDisconnected).await;
    }

    async fn run_inner(&mut self) -> io::Result<()> {
        println!("Connecting to signaling server...");

        let tcp_stream = TcpStream::connect(TCP_SERVER).await?;

        println!("TCP signaling connected");

        /*
         * 하나의 UDP socket을
         *
         * 1. signaling UDP 등록
         * 2. hole punching
         * 3. 실제 P2P 채팅
         *
         * 모두에 사용한다.
         */
        let p2p = P2pState {
            udp_socket: Arc::new(UdpSocket::bind("0.0.0.0:0").await?),
            peer_addr: Arc::new(RwLock::new(None)),
            connected: Arc::new(AtomicBool::new(false)),
            session_id: Arc::new(AtomicU64::new(0)),
        };
        println!("Local UDP socket: {}", p2p.udp_socket.local_addr()?);

        /*
         * hole punching task가
         * 10번 시도 후 실패했음을
         * NetworkClient::run에 알리는 channel.
         */
        let (failed_tx, mut failed_rx) = mpsc::channel::<(u64, SocketAddr)>(1);
        let (reader, mut writer) = tcp_stream.into_split();
        let mut reader = BufReader::new(reader);

        /*
         * 서버가 TCP 연결 직후
         *
         * CLIENT_ID xxx
         *
         * 를 전송한다.
         */
        let client_id = read_client_id(&mut reader).await?;
        println!("Client ID: {client_id}");

        /*
         * UDP signaling 서버 IPv4 주소.
         *
         * 현재 프로젝트는 IPv4 hole punching 단계라
         * IPv4 주소만 선택한다.
         */
        let udp_server_addr = lookup_host(UDP_SERVER)
            .await?
            .find(|addr| addr.is_ipv4())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::AddrNotAvailable,
                    "UDP server IPv4 address not found",
                )
            })?;

        println!("UDP signaling server: {udp_server_addr}");

        /*
         * TCP server 연결 완료를 GUI에 전달.
         */
        let _ = self.event_tx.send(NetworkEvent::ServerConnected).await;

        /*
         * UDP 수신 task.
         *
         * PUNCH / PUNCH_ACK / 실제 채팅을
         * 계속 기다린다.
         */
        {
            let p2p_clone = p2p.clone();

            let event_tx_clone = self.event_tx.clone();

            tokio::spawn(async move {
                udp_receive_loop(p2p_clone, event_tx_clone).await;
            });
        }

        /*
         * 이 loop가 이제 예전의
         *
         * - stdin 입력
         * - signaling 수신
         * - P2P 실패 감지
         *
         * 역할을 모두 담당한다.
         */
        loop {
            let mut server_line = String::new();

            tokio::select! {
                /*
                 * GUI -> Network
                 */
                command =
                    self.command_rx.recv() =>
                {
                    let Some(command) =
                        command
                    else {
                        break;
                    };

                    handle_command(command, &mut writer, &p2p, &self.event_tx).await?;
                }

                /*
                 * signaling server -> Network
                 */
                result = reader.read_line(&mut server_line) =>
                {
                    let size = result?;

                    if size == 0 {
                        println!("Signaling server disconnected");
                        break;
                    }

                    self.handle_server_message(
                            server_line.trim(),
                            &p2p,
                            &failed_tx,
                            &client_id,
                            udp_server_addr,
                        )
                        .await?;
                }

                /*
                 * hole punching 10회 실패.
                 */
                result =
                    failed_rx.recv() =>
                {
                    let Some((session_id, failed_peer)) = result else {
                        continue;
                    };

                    if !p2p.is_current_session(session_id)
                        || p2p.get_peer().await != Some(failed_peer)
                        || p2p.is_connected()
                    {
                        continue;
                    }

                    println!("P2P connection failed");

                    p2p.reset().await;

                    let _ = writer.write_all(b"P2P_FAILED\n").await;
                    let _ = self.event_tx.send(NetworkEvent::P2pFailed).await;

                    /*
                     * 예전에는 여기서 break해서
                     * 프로그램 자체를 끝냈지만,
                     *
                     * GUI 버전에서는 TCP signaling
                     * 연결은 유지한다.
                     */
                }
            }
        }

        Ok(())
    }

    async fn handle_server_message(
        &self,
        message: &str,
        p2p: &P2pState,
        failed_tx: &mpsc::Sender<(u64, SocketAddr)>,
        client_id: &str,
        udp_server_addr: SocketAddr,
    ) -> io::Result<()> {
        println!("[SERVER] {message}");

        let mut parts = message.split_whitespace();

        match parts.next() {
            Some("CREATED") => {
                let Some(room_code) = parts.next() else {
                    return Ok(());
                };

                register_udp(&p2p.udp_socket, client_id, udp_server_addr).await?;

                let _ = self
                    .event_tx
                    .send(NetworkEvent::RoomCreated(room_code.to_string()))
                    .await;
            }

            Some("JOINED") => {
                let Some(room_code) = parts.next() else {
                    return Ok(());
                };

                register_udp(&p2p.udp_socket, client_id, udp_server_addr).await?;

                let _ = self
                    .event_tx
                    .send(NetworkEvent::JoinedRoom(room_code.to_string()))
                    .await;
            }

            Some("PEER") => {
                let Some(addr) = parts.next() else {
                    return Ok(());
                };

                let Ok(addr) = addr.parse::<SocketAddr>() else {
                    return Ok(());
                };

                let session_id = p2p.start_session();
                p2p.set_peer(Some(addr)).await;
                p2p.set_connected(false);

                start_hole_punching(p2p.clone(), session_id, addr, failed_tx.clone());
            }

            Some("PEER_LEFT") => {
                p2p.reset().await;

                let _ = self.event_tx.send(NetworkEvent::PeerDisconnected).await;
            }

            Some("ROOM_FULL") => {
                let _ = self.event_tx.send(NetworkEvent::RoomFull).await;
            }

            Some("ROOM_NOT_FOUND") => {
                let _ = self.event_tx.send(NetworkEvent::RoomNotFound).await;
            }

            Some("ALREADY_IN_ROOM") | Some("ALREADY_JOINED") => {
                let _ = self.event_tx.send(NetworkEvent::AlreadyInRoom).await;
            }

            Some("P2P_DISCONNECTED") => {
                p2p.reset().await;

                let _ = self.event_tx.send(NetworkEvent::PeerDisconnected).await;
            }

            Some("LEFT") => {
                p2p.reset().await;
            }

            _ => {}
        }

        Ok(())
    }
}

async fn handle_command(
    command: NetworkCommand,
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    p2p: &P2pState,
    event_tx: &mpsc::Sender<NetworkEvent>,
) -> io::Result<()> {
    match command {
        NetworkCommand::CreateRoom => {
            writer.write_all(b"CREATE\n").await?;
        }

        NetworkCommand::JoinRoom(room_code) => {
            let room_code = room_code.trim().to_uppercase();

            if room_code.is_empty() {
                return Ok(());
            }

            let message = format!("JOIN {room_code}\n");

            writer.write_all(message.as_bytes()).await?;
        }

        NetworkCommand::LeaveRoom => {
            writer.write_all(b"LEAVE\n").await?;
            p2p.reset().await;
        }

        NetworkCommand::SendMessage(message) => {
            if !p2p.is_connected() {
                let _ = event_tx
                    .send(NetworkEvent::Error(
                        "P2P 연결이 없어 메시지를 전송하지 못했습니다.".to_string(),
                    ))
                    .await;
                return Ok(());
            }

            let peer = p2p.get_peer().await;

            let Some(peer) = peer else {
                let _ = event_tx
                    .send(NetworkEvent::Error(
                        "상대 주소가 없어 메시지를 전송하지 못했습니다.".to_string(),
                    ))
                    .await;
                return Ok(());
            };

            match p2p.udp_socket.send_to(message.as_bytes(), peer).await {
                Ok(_) => {
                    println!("[ME] {message}");
                    let _ = event_tx.send(NetworkEvent::MessageSent(message)).await;
                }
                Err(error) => {
                    eprintln!("P2P message send error: {error}");
                    let _ = event_tx
                        .send(NetworkEvent::Error(format!(
                            "P2P 메시지를 전송하지 못했습니다: {error}"
                        )))
                        .await;
                }
            }
        }
    }

    Ok(())
}

/*
 * TCP 연결 직후 서버가 보내는
 *
 * CLIENT_ID xxx
 *
 * 파싱.
 */
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

    let mut parts = line.split_whitespace();

    match (parts.next(), parts.next()) {
        (Some("CLIENT_ID"), Some(id)) => Ok(id.to_string()),

        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid CLIENT_ID response",
        )),
    }
}

/*
 * 방 생성 또는 입장 성공 후
 * signaling UDP server에
 * 현재 UDP endpoint 등록.
 */
async fn register_udp(
    udp_socket: &UdpSocket,
    client_id: &str,
    udp_server_addr: SocketAddr,
) -> io::Result<()> {
    let message = format!("REGISTER {client_id}");

    udp_socket
        .send_to(message.as_bytes(), udp_server_addr)
        .await?;

    println!("UDP endpoint registration sent");

    Ok(())
}

/*
 * NAT hole punching.
 */
fn start_hole_punching(
    p2p: P2pState,
    session_id: u64,
    peer_addr: SocketAddr,
    failed_tx: mpsc::Sender<(u64, SocketAddr)>,
) {
    tokio::spawn(async move {
        println!("Starting hole punching...");

        for attempt in 1..=10 {
            if p2p.is_connected() {
                println!("Hole punching stopped: already connected");
                return;
            }

            if !p2p.is_current_session(session_id) || p2p.get_peer().await != Some(peer_addr) {
                println!("Hole punching stopped: peer session changed");
                return;
            }

            match p2p.udp_socket.send_to(b"PUNCH", peer_addr).await {
                Ok(_) => {
                    println!("PUNCH #{attempt} -> {peer_addr}");
                }
                Err(error) => {
                    eprintln!("Punch send error: {error}");
                }
            }

            sleep(Duration::from_millis(250)).await;
        }

        if p2p.is_current_session(session_id)
            && !p2p.is_connected()
            && p2p.get_peer().await == Some(peer_addr)
        {
            println!("P2P connection failed");
            let _ = failed_tx.send((session_id, peer_addr)).await;
        }
    });
}

/*
 * 모든 UDP 수신 담당.
 *
 * - PUNCH
 * - PUNCH_ACK
 * - 실제 P2P 메시지
 */
async fn udp_receive_loop(p2p: P2pState, event_tx: mpsc::Sender<NetworkEvent>) {
    let mut buffer = [0u8; 2048];

    loop {
        let (size, from) = match p2p.udp_socket.recv_from(&mut buffer).await {
            Ok(result) => result,
            Err(error) => {
                eprintln!("UDP receive error: {error}");
                break;
            }
        };

        let message = String::from_utf8_lossy(&buffer[..size]);

        match message.as_ref() {
            "PUNCH" => {
                if p2p.get_peer().await != Some(from) {
                    continue;
                }

                println!("PUNCH received from {from}");
                let _ = p2p.udp_socket.send_to(b"PUNCH_ACK", from).await;
            }

            "PUNCH_ACK" => {
                /*
                 * 첫 ACK에서만 GUI에
                 * PeerConnected 이벤트를 보낸다.
                 */
                if p2p.mark_connected_if_peer(from, &event_tx).await {
                    println!("P2P connected: {from}");
                }
            }

            _ => {
                /*
                 * hole punching control packet이
                 * 아니라면 실제 채팅 메시지.
                 */

                /*
                 * 알려진 peer가 아닌 곳에서 온
                 * 패킷은 채팅으로 처리하지 않는다.
                 */
                let current_peer = p2p.get_peer().await;

                if !p2p.is_connected() || current_peer != Some(from) {
                    continue;
                }

                println!("[P2P {from}] {message}");

                let _ = event_tx
                    .send(NetworkEvent::MessageReceived(message.to_string()))
                    .await;
            }
        }
    }
}

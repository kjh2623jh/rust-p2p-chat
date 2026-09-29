use super::{NetworkCommand, NetworkError, NetworkEvent};
use crate::protocol::{
    InviteCode, MAX_DATAGRAM, MAX_MESSAGE_BYTES, MAX_MESSAGE_CHARS, MAX_SIGNAL_LINE,
    PROTOCOL_VERSION, decode_hex, encode_hex,
};
use hmac::{Hmac, Mac};
use rand::RngCore;
use sha2::Sha256;
use snow::{Builder, HandshakeState, StatelessTransportState, params::NoiseParams};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    env, io,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpStream, UdpSocket, lookup_host},
    sync::mpsc,
    time::{interval, sleep, timeout},
};
use tokio_rustls::{
    TlsConnector,
    client::TlsStream,
    rustls::{ClientConfig, RootCertStore, pki_types::ServerName, version::TLS13},
};
use zeroize::Zeroizing;

type HmacSha256 = Hmac<Sha256>;

const DEFAULT_SERVER_HOST: &str = "p2psignal.mcv.kr";
const DEFAULT_TCP_PORT: u16 = 9000;
const DEFAULT_UDP_PORT: u16 = 9001;
const NOISE_PATTERN: &str = "Noise_NNpsk0_25519_ChaChaPoly_SHA256";
const MAGIC: &[u8; 4] = b"P2P2";
const PACKET_PUNCH: u8 = 0;
const PACKET_HANDSHAKE_INIT: u8 = 1;
const PACKET_HANDSHAKE_RESPONSE: u8 = 2;
const PACKET_TRANSPORT: u8 = 3;
const PLAIN_READY: u8 = 1;
const PLAIN_READY_ACK: u8 = 2;
const PLAIN_MESSAGE: u8 = 3;
const PLAIN_MESSAGE_ACK: u8 = 4;
const PLAIN_HEARTBEAT: u8 = 5;
const MAX_PENDING_MESSAGES: usize = 64;
const MAX_HISTORY_DEDUP: usize = 512;

pub struct NetworkClient {
    command_rx: mpsc::Receiver<NetworkCommand>,
    event_tx: mpsc::Sender<NetworkEvent>,
}

enum RunOutcome {
    CommandChannelClosed,
    ServerDisconnected,
}

struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct RoomContext {
    invite: InviteCode,
    room_id: [u8; 16],
    session_nonce: [u8; 16],
}

struct RegistrationAttempt {
    nonce: [u8; 16],
    started_at: Instant,
    last_probe: Instant,
}

struct PendingMessage {
    text: String,
    retries: usize,
    next_retry: Instant,
}

#[derive(Default)]
struct ReplayWindow {
    highest: Option<u64>,
    bitmap: u128,
}

impl ReplayWindow {
    fn would_accept(&self, sequence: u64) -> bool {
        let Some(highest) = self.highest else {
            return true;
        };
        if sequence > highest {
            return true;
        }
        let distance = highest - sequence;
        distance < 128 && self.bitmap & (1_u128 << distance) == 0
    }

    fn record(&mut self, sequence: u64) {
        match self.highest {
            None => {
                self.highest = Some(sequence);
                self.bitmap = 1;
            }
            Some(highest) if sequence > highest => {
                let shift = sequence - highest;
                self.bitmap = if shift >= 128 {
                    1
                } else {
                    (self.bitmap << shift) | 1
                };
                self.highest = Some(sequence);
            }
            Some(highest) => self.bitmap |= 1_u128 << (highest - sequence),
        }
    }
}

struct SecurePeer {
    address: SocketAddr,
    initiator: bool,
    handshake: Option<HandshakeState>,
    transport: Option<StatelessTransportState>,
    initial_packet: Option<Vec<u8>>,
    response_packet: Option<Vec<u8>>,
    send_sequence: u64,
    replay: ReplayWindow,
    ready_received: bool,
    ready_acked: bool,
    connected: bool,
    started_at: Instant,
    last_handshake_send: Instant,
    last_ready_send: Instant,
    last_received: Instant,
    last_heartbeat_send: Instant,
    pending: HashMap<u64, PendingMessage>,
    next_message_id: u64,
    received_ids: HashSet<u64>,
    received_order: VecDeque<u64>,
    rate_started: Instant,
    packets_this_second: u32,
}

impl SecurePeer {
    fn new(address: SocketAddr, initiator: bool, room: &RoomContext) -> Result<Self, NetworkError> {
        let params: NoiseParams = NOISE_PATTERN
            .parse()
            .map_err(|_| NetworkError::ProtocolMismatch)?;
        let mut prologue = b"p2p-chat/v2/noise".to_vec();
        prologue.extend_from_slice(&room.room_id);
        prologue.extend_from_slice(&room.session_nonce);
        let builder = Builder::new(params)
            .prologue(&prologue)
            .and_then(|builder| builder.psk(0, room.invite.secret()))
            .map_err(|_| NetworkError::PeerAuthenticationFailed)?;
        let mut handshake = if initiator {
            builder.build_initiator()
        } else {
            builder.build_responder()
        }
        .map_err(|_| NetworkError::PeerAuthenticationFailed)?;

        let initial_packet = if initiator {
            let mut output = vec![0_u8; MAX_DATAGRAM - 5];
            let size = handshake
                .write_message(&[], &mut output)
                .map_err(|_| NetworkError::PeerAuthenticationFailed)?;
            output.truncate(size);
            Some(wrap_packet(PACKET_HANDSHAKE_INIT, &output))
        } else {
            None
        };
        let now = Instant::now();
        Ok(Self {
            address,
            initiator,
            handshake: Some(handshake),
            transport: None,
            initial_packet,
            response_packet: None,
            send_sequence: 0,
            replay: ReplayWindow::default(),
            ready_received: false,
            ready_acked: false,
            connected: false,
            started_at: now,
            last_handshake_send: now - Duration::from_secs(1),
            last_ready_send: now - Duration::from_secs(1),
            last_received: now,
            last_heartbeat_send: now,
            pending: HashMap::new(),
            next_message_id: 1,
            received_ids: HashSet::new(),
            received_order: VecDeque::new(),
            rate_started: now,
            packets_this_second: 0,
        })
    }

    fn allow_packet(&mut self) -> bool {
        let now = Instant::now();
        if now.duration_since(self.rate_started) >= Duration::from_secs(1) {
            self.rate_started = now;
            self.packets_this_second = 0;
        }
        if self.packets_this_second >= 100 {
            return false;
        }
        self.packets_this_second += 1;
        true
    }

    fn encrypt(&mut self, plaintext: &[u8]) -> Result<Vec<u8>, NetworkError> {
        let transport = self
            .transport
            .as_ref()
            .ok_or(NetworkError::PeerAuthenticationFailed)?;
        let sequence = self.send_sequence;
        self.send_sequence = self
            .send_sequence
            .checked_add(1)
            .ok_or(NetworkError::PeerAuthenticationFailed)?;
        let mut ciphertext = vec![0_u8; plaintext.len() + 32];
        let size = transport
            .write_message(sequence, plaintext, &mut ciphertext)
            .map_err(|_| NetworkError::PeerAuthenticationFailed)?;
        ciphertext.truncate(size);
        let mut packet = Vec::with_capacity(13 + ciphertext.len());
        packet.extend_from_slice(MAGIC);
        packet.push(PACKET_TRANSPORT);
        packet.extend_from_slice(&sequence.to_be_bytes());
        packet.extend_from_slice(&ciphertext);
        if packet.len() > MAX_DATAGRAM {
            return Err(NetworkError::MessageTooLong);
        }
        Ok(packet)
    }

    fn decrypt(&mut self, packet: &[u8]) -> Result<Option<Vec<u8>>, NetworkError> {
        if packet.len() < 13 {
            return Ok(None);
        }
        let sequence = u64::from_be_bytes(packet[5..13].try_into().expect("slice length"));
        if !self.replay.would_accept(sequence) {
            return Ok(None);
        }
        let transport = self
            .transport
            .as_ref()
            .ok_or(NetworkError::PeerAuthenticationFailed)?;
        let mut plaintext = vec![0_u8; packet.len()];
        let Ok(size) = transport.read_message(sequence, &packet[13..], &mut plaintext) else {
            return Ok(None);
        };
        plaintext.truncate(size);
        self.replay.record(sequence);
        Ok(Some(plaintext))
    }
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
        loop {
            match self.run_inner().await {
                Ok(RunOutcome::CommandChannelClosed) => return,
                Ok(RunOutcome::ServerDisconnected) => {}
                Err(error) => {
                    let _ = self
                        .event_tx
                        .send(NetworkEvent::Error(NetworkError::Transport(safe_io_error(
                            &error,
                        ))))
                        .await;
                }
            }
            let _ = self.event_tx.send(NetworkEvent::ServerDisconnected).await;
            if self.command_rx.is_closed() {
                return;
            }
            sleep(Duration::from_secs(3)).await;
        }
    }

    async fn run_inner(&mut self) -> io::Result<RunOutcome> {
        let (host, tcp_address, udp_address) = server_addresses();
        let stream = match connect_tls(&host, &tcp_address).await {
            Ok(stream) => stream,
            Err(error) => {
                let _ = self
                    .event_tx
                    .send(NetworkEvent::Error(NetworkError::TlsFailed(safe_io_error(
                        &error,
                    ))))
                    .await;
                return Ok(RunOutcome::ServerDisconnected);
            }
        };
        let udp_server = lookup_host(&udp_address)
            .await?
            .find(SocketAddr::is_ipv4)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::AddrNotAvailable,
                    "UDP server has no IPv4 address",
                )
            })?;
        let udp = UdpSocket::bind("0.0.0.0:0").await?;
        let (mut reader, mut writer) = tokio::io::split(stream);

        let hello = timeout(Duration::from_secs(5), read_bounded_line(&mut reader))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "server HELLO timed out"))??
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::UnexpectedEof, "server closed before HELLO")
            })?;
        let (client_id, registration_secret) = match parse_hello(&hello) {
            Some((client_id, secret)) => (client_id, Zeroizing::new(secret)),
            None => {
                let _ = self
                    .event_tx
                    .send(NetworkEvent::Error(NetworkError::ProtocolMismatch))
                    .await;
                return Ok(RunOutcome::ServerDisconnected);
            }
        };

        // A dedicated reader keeps partially received TLS lines intact while the main loop ticks.
        let (server_line_tx, mut server_line_rx) = mpsc::channel::<io::Result<String>>(32);
        let _server_reader = AbortOnDrop(tokio::spawn(async move {
            loop {
                match read_bounded_line(&mut reader).await {
                    Ok(Some(line)) => {
                        if server_line_tx.send(Ok(line)).await.is_err() {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(error) => {
                        let _ = server_line_tx.send(Err(error)).await;
                        break;
                    }
                }
            }
        }));

        let _ = self.event_tx.send(NetworkEvent::ServerConnected).await;
        let mut room: Option<RoomContext> = None;
        let mut pending_invite: Option<InviteCode> = None;
        let mut registration: Option<RegistrationAttempt> = None;
        let mut peer: Option<SecurePeer> = None;
        let mut timer = interval(Duration::from_millis(100));
        let mut udp_buffer = [0_u8; MAX_DATAGRAM + 1];

        loop {
            tokio::select! {
                command = self.command_rx.recv() => {
                    let Some(command) = command else { return Ok(RunOutcome::CommandChannelClosed) };
                    match command {
                        NetworkCommand::CreateRoom => {
                            if room.is_none() && pending_invite.is_none() {
                                let invite = InviteCode::generate();
                                writer.write_all(format!("CREATE {}\n", invite.room_id_hex()).as_bytes()).await?;
                                pending_invite = Some(invite);
                            }
                        }
                        NetworkCommand::JoinRoom(invite) => {
                            if room.is_none() && pending_invite.is_none() {
                                writer.write_all(format!("JOIN {}\n", invite.room_id_hex()).as_bytes()).await?;
                                pending_invite = Some(invite);
                            }
                        }
                        NetworkCommand::LeaveRoom => {
                            writer.write_all(b"LEAVE\n").await?;
                            fail_pending(&mut peer, &self.event_tx, "대화방에서 나갔습니다.").await;
                            room = None;
                            pending_invite = None;
                            registration = None;
                            peer = None;
                        }
                        NetworkCommand::SendMessage(text) => {
                            if text.chars().count() > MAX_MESSAGE_CHARS || text.len() > MAX_MESSAGE_BYTES {
                                let _ = self.event_tx.send(NetworkEvent::Error(NetworkError::MessageTooLong)).await;
                                continue;
                            }
                            let Some(active_peer) = peer.as_mut().filter(|peer| peer.connected) else {
                                let _ = self.event_tx.send(NetworkEvent::Error(NetworkError::Transport(
                                    "상대방과 연결되어 있지 않습니다.".to_owned()
                                ))).await;
                                continue;
                            };
                            if active_peer.pending.len() >= MAX_PENDING_MESSAGES {
                                let _ = self.event_tx.send(NetworkEvent::Error(NetworkError::RateLimited)).await;
                                continue;
                            }
                            let id = active_peer.next_message_id;
                            active_peer.next_message_id = active_peer.next_message_id.wrapping_add(1).max(1);
                            let packet = encrypted_message_packet(active_peer, id, &text)
                                .map_err(network_error_to_io)?;
                            udp.send_to(&packet, active_peer.address).await?;
                            active_peer.pending.insert(id, PendingMessage {
                                text: text.clone(), retries: 0,
                                next_retry: Instant::now() + Duration::from_millis(500),
                            });
                            let _ = self.event_tx.send(NetworkEvent::MessagePending { id, text }).await;
                        }
                    }
                }
                line = server_line_rx.recv() => {
                    let Some(line) = line else { return Ok(RunOutcome::ServerDisconnected) };
                    let line = line?;
                    let action = handle_server_line(
                        &line, &client_id, &registration_secret, udp_server, &udp,
                        &mut writer, &mut room, &mut pending_invite,
                        &mut registration, &mut peer, &self.event_tx,
                    ).await?;
                    if action == ServerAction::Disconnected {
                        return Ok(RunOutcome::ServerDisconnected);
                    }
                }
                received = udp.recv_from(&mut udp_buffer) => {
                    let (size, from) = received?;
                    if size > MAX_DATAGRAM { continue; }
                    let Some(active_peer) = peer.as_mut() else { continue };
                    if from != active_peer.address || !active_peer.allow_packet() { continue; }
                    if let Err(error) = handle_peer_packet(
                        &udp_buffer[..size], active_peer, &udp, &self.event_tx
                    ).await {
                        let _ = self.event_tx.send(NetworkEvent::Error(error)).await;
                        writer.write_all(b"P2P_FAILED\n").await?;
                        fail_pending(&mut peer, &self.event_tx, "보안 연결이 종료되었습니다.").await;
                        peer = None;
                        room = None;
                        let _ = self.event_tx.send(NetworkEvent::P2pFailed).await;
                    }
                }
                _ = timer.tick() => {
                    if let Some(attempt) = registration.as_mut() {
                        let now = Instant::now();
                        if now.duration_since(attempt.started_at) >= Duration::from_secs(10) {
                            writer.write_all(b"P2P_FAILED\n").await?;
                            registration = None;
                            room = None;
                            let _ = self.event_tx.send(NetworkEvent::P2pFailed).await;
                            continue;
                        }
                        if now.duration_since(attempt.last_probe) >= Duration::from_secs(1) {
                            udp.send_to(
                                format!("PROBE {client_id} {}", encode_hex(&attempt.nonce)).as_bytes(),
                                udp_server,
                            ).await?;
                            attempt.last_probe = now;
                        }
                    }
                    if tick_peer(&mut peer, &udp, &self.event_tx).await? {
                        writer.write_all(b"P2P_FAILED\n").await?;
                        fail_pending(&mut peer, &self.event_tx, "상대방의 응답이 없습니다.").await;
                        peer = None;
                        room = None;
                        let _ = self.event_tx.send(NetworkEvent::P2pFailed).await;
                    }
                }
            }
        }
    }
}

#[derive(PartialEq, Eq)]
enum ServerAction {
    Continue,
    Disconnected,
}

#[allow(clippy::too_many_arguments)]
async fn handle_server_line<W: AsyncWrite + Unpin>(
    line: &str,
    client_id: &str,
    registration_secret: &[u8; 32],
    udp_server: SocketAddr,
    udp: &UdpSocket,
    writer: &mut W,
    room: &mut Option<RoomContext>,
    pending_invite: &mut Option<InviteCode>,
    registration: &mut Option<RegistrationAttempt>,
    peer: &mut Option<SecurePeer>,
    event_tx: &mpsc::Sender<NetworkEvent>,
) -> io::Result<ServerAction> {
    let mut parts = line.split_whitespace();
    match parts.next() {
        Some("PING") => writer.write_all(b"PONG\n").await?,
        Some("CREATED") | Some("JOINED") => {
            let created = line.starts_with("CREATED ");
            let (Some(room_id), Some(session_nonce)) = (parts.next(), parts.next()) else {
                send_protocol_error(event_tx).await;
                return Ok(ServerAction::Continue);
            };
            let (Some(room_id), Some(session_nonce), Some(invite)) = (
                decode_hex::<16>(room_id),
                decode_hex::<16>(session_nonce),
                pending_invite.take(),
            ) else {
                send_protocol_error(event_tx).await;
                return Ok(ServerAction::Continue);
            };
            if invite.room_id() != room_id {
                send_protocol_error(event_tx).await;
                return Ok(ServerAction::Continue);
            }
            let fingerprint = invite.fingerprint();
            let exposed = created.then(|| invite.expose());
            *room = Some(RoomContext {
                invite,
                room_id,
                session_nonce,
            });
            let nonce = random_bytes::<16>();
            *registration = Some(RegistrationAttempt {
                nonce,
                started_at: Instant::now(),
                last_probe: Instant::now(),
            });
            udp.send_to(
                format!("PROBE {client_id} {}", encode_hex(&nonce)).as_bytes(),
                udp_server,
            )
            .await?;
            if let Some(invite) = exposed {
                let _ = event_tx
                    .send(NetworkEvent::RoomCreated {
                        invite,
                        fingerprint,
                    })
                    .await;
            } else {
                let _ = event_tx
                    .send(NetworkEvent::JoinedRoom { fingerprint })
                    .await;
            }
        }
        Some("UDP_CHALLENGE") => {
            let (Some(observed), Some(client_nonce), Some(server_nonce)) =
                (parts.next(), parts.next(), parts.next())
            else {
                return Ok(ServerAction::Continue);
            };
            let (Ok(observed), Some(client_nonce), Some(server_nonce)) = (
                observed.parse::<SocketAddr>(),
                decode_hex::<16>(client_nonce),
                decode_hex::<16>(server_nonce),
            ) else {
                return Ok(ServerAction::Continue);
            };
            if registration.as_ref().map(|attempt| &attempt.nonce) != Some(&client_nonce) {
                return Ok(ServerAction::Continue);
            }
            let payload = registration_payload(client_id, &client_nonce, &server_nonce, observed);
            let mut mac = HmacSha256::new_from_slice(registration_secret).expect("HMAC key length");
            mac.update(payload.as_bytes());
            let tag = mac.finalize().into_bytes();
            udp.send_to(
                format!(
                    "REGISTER {client_id} {} {} {}",
                    encode_hex(&client_nonce),
                    encode_hex(&server_nonce),
                    encode_hex(&tag)
                )
                .as_bytes(),
                udp_server,
            )
            .await?;
        }
        Some("REGISTERED") => *registration = None,
        Some("PEER") => {
            let (Some(address), Some(role), Some(nonce)) =
                (parts.next(), parts.next(), parts.next())
            else {
                send_protocol_error(event_tx).await;
                return Ok(ServerAction::Continue);
            };
            let (Ok(address), Some(nonce), Some(room_context)) = (
                address.parse::<SocketAddr>(),
                decode_hex::<16>(nonce),
                room.as_ref(),
            ) else {
                send_protocol_error(event_tx).await;
                return Ok(ServerAction::Continue);
            };
            if nonce != room_context.session_nonce || !matches!(role, "I" | "R") {
                send_protocol_error(event_tx).await;
                return Ok(ServerAction::Continue);
            }
            match SecurePeer::new(address, role == "I", room_context) {
                Ok(new_peer) => {
                    *registration = None;
                    *peer = Some(new_peer);
                }
                Err(error) => {
                    let _ = event_tx.send(NetworkEvent::Error(error)).await;
                }
            }
        }
        Some("PEER_LEFT") | Some("ROOM_EXPIRED") => {
            writer.write_all(b"LEAVE\n").await?;
            fail_pending(peer, event_tx, "상대방과 연결이 종료되었습니다.").await;
            *registration = None;
            *peer = None;
            *room = None;
            let _ = event_tx.send(NetworkEvent::PeerDisconnected).await;
        }
        Some("ROOM_FULL") => {
            *pending_invite = None;
            let _ = event_tx.send(NetworkEvent::RoomFull).await;
        }
        Some("ROOM_NOT_FOUND") => {
            *pending_invite = None;
            let _ = event_tx.send(NetworkEvent::RoomNotFound).await;
        }
        Some("ALREADY_IN_ROOM") => {
            *pending_invite = None;
            let _ = event_tx.send(NetworkEvent::AlreadyInRoom).await;
        }
        Some("RATE_LIMITED") => {
            *pending_invite = None;
            let _ = event_tx
                .send(NetworkEvent::Error(NetworkError::RateLimited))
                .await;
        }
        Some("LEFT") => {}
        Some("HELLO") => {
            send_protocol_error(event_tx).await;
        }
        Some("ERROR") | Some("ROOM_EXISTS") => {
            *pending_invite = None;
            let _ = event_tx
                .send(NetworkEvent::Error(NetworkError::Transport(
                    "서버가 요청을 처리하지 못했습니다.".to_owned(),
                )))
                .await;
        }
        None => {}
        _ => send_protocol_error(event_tx).await,
    }
    Ok(ServerAction::Continue)
}

async fn handle_peer_packet(
    packet: &[u8],
    peer: &mut SecurePeer,
    udp: &UdpSocket,
    event_tx: &mpsc::Sender<NetworkEvent>,
) -> Result<(), NetworkError> {
    if packet.len() < 5 || &packet[..4] != MAGIC {
        return Ok(());
    }
    peer.last_received = Instant::now();
    match packet[4] {
        PACKET_PUNCH => {
            if let Some(initial) = &peer.initial_packet {
                let _ = udp.send_to(initial, peer.address).await;
            }
        }
        PACKET_HANDSHAKE_INIT if !peer.initiator => {
            if let Some(response) = &peer.response_packet {
                let _ = udp.send_to(response, peer.address).await;
                return Ok(());
            }
            let mut handshake = peer
                .handshake
                .take()
                .ok_or(NetworkError::PeerAuthenticationFailed)?;
            let mut scratch = [0_u8; MAX_DATAGRAM];
            handshake
                .read_message(&packet[5..], &mut scratch)
                .map_err(|_| NetworkError::PeerAuthenticationFailed)?;
            let mut response = vec![0_u8; MAX_DATAGRAM - 5];
            let size = handshake
                .write_message(&[], &mut response)
                .map_err(|_| NetworkError::PeerAuthenticationFailed)?;
            response.truncate(size);
            let response = wrap_packet(PACKET_HANDSHAKE_RESPONSE, &response);
            udp.send_to(&response, peer.address)
                .await
                .map_err(|error| NetworkError::Transport(safe_io_error(&error)))?;
            peer.response_packet = Some(response);
            peer.transport = Some(
                handshake
                    .into_stateless_transport_mode()
                    .map_err(|_| NetworkError::PeerAuthenticationFailed)?,
            );
            send_ready(peer, udp).await?;
        }
        PACKET_HANDSHAKE_RESPONSE if peer.initiator && peer.transport.is_none() => {
            let mut handshake = peer
                .handshake
                .take()
                .ok_or(NetworkError::PeerAuthenticationFailed)?;
            let mut scratch = [0_u8; MAX_DATAGRAM];
            handshake
                .read_message(&packet[5..], &mut scratch)
                .map_err(|_| NetworkError::PeerAuthenticationFailed)?;
            peer.transport = Some(
                handshake
                    .into_stateless_transport_mode()
                    .map_err(|_| NetworkError::PeerAuthenticationFailed)?,
            );
            send_ready(peer, udp).await?;
        }
        PACKET_TRANSPORT if peer.transport.is_some() => {
            let Some(plaintext) = peer.decrypt(packet)? else {
                return Ok(());
            };
            handle_plaintext(&plaintext, peer, udp, event_tx).await?;
        }
        _ => {}
    }
    Ok(())
}

async fn handle_plaintext(
    plaintext: &[u8],
    peer: &mut SecurePeer,
    udp: &UdpSocket,
    event_tx: &mpsc::Sender<NetworkEvent>,
) -> Result<(), NetworkError> {
    match plaintext.first().copied() {
        Some(PLAIN_READY) => {
            peer.ready_received = true;
            let packet = peer.encrypt(&[PLAIN_READY_ACK])?;
            udp.send_to(&packet, peer.address)
                .await
                .map_err(|error| NetworkError::Transport(safe_io_error(&error)))?;
            maybe_mark_connected(peer, event_tx).await;
        }
        Some(PLAIN_READY_ACK) => {
            peer.ready_acked = true;
            maybe_mark_connected(peer, event_tx).await;
        }
        Some(PLAIN_MESSAGE) if plaintext.len() >= 9 => {
            let id = u64::from_be_bytes(plaintext[1..9].try_into().expect("slice length"));
            let text = std::str::from_utf8(&plaintext[9..])
                .map_err(|_| NetworkError::PeerAuthenticationFailed)?;
            if text.chars().count() > MAX_MESSAGE_CHARS || text.len() > MAX_MESSAGE_BYTES {
                return Err(NetworkError::PeerAuthenticationFailed);
            }
            let mut ack = vec![PLAIN_MESSAGE_ACK];
            ack.extend_from_slice(&id.to_be_bytes());
            let packet = peer.encrypt(&ack)?;
            udp.send_to(&packet, peer.address)
                .await
                .map_err(|error| NetworkError::Transport(safe_io_error(&error)))?;
            if peer.received_ids.insert(id) {
                peer.received_order.push_back(id);
                if peer.received_order.len() > MAX_HISTORY_DEDUP
                    && let Some(old) = peer.received_order.pop_front()
                {
                    peer.received_ids.remove(&old);
                }
                let _ = event_tx
                    .send(NetworkEvent::MessageReceived(text.to_owned()))
                    .await;
            }
        }
        Some(PLAIN_MESSAGE_ACK) if plaintext.len() == 9 => {
            let id = u64::from_be_bytes(plaintext[1..9].try_into().expect("slice length"));
            if peer.pending.remove(&id).is_some() {
                let _ = event_tx.send(NetworkEvent::MessageDelivered(id)).await;
            }
        }
        Some(PLAIN_HEARTBEAT) => {}
        _ => return Err(NetworkError::PeerAuthenticationFailed),
    }
    Ok(())
}

async fn maybe_mark_connected(peer: &mut SecurePeer, event_tx: &mpsc::Sender<NetworkEvent>) {
    if !peer.connected && peer.ready_received && peer.ready_acked {
        peer.connected = true;
        let _ = event_tx.send(NetworkEvent::PeerConnected).await;
    }
}

async fn send_ready(peer: &mut SecurePeer, udp: &UdpSocket) -> Result<(), NetworkError> {
    let packet = peer.encrypt(&[PLAIN_READY])?;
    udp.send_to(&packet, peer.address)
        .await
        .map_err(|error| NetworkError::Transport(safe_io_error(&error)))?;
    peer.last_ready_send = Instant::now();
    Ok(())
}

async fn tick_peer(
    peer: &mut Option<SecurePeer>,
    udp: &UdpSocket,
    event_tx: &mpsc::Sender<NetworkEvent>,
) -> io::Result<bool> {
    let Some(peer) = peer.as_mut() else {
        return Ok(false);
    };
    let now = Instant::now();
    if !peer.connected && now.duration_since(peer.started_at) > Duration::from_secs(10) {
        return Ok(true);
    }
    if peer.transport.is_none()
        && now.duration_since(peer.last_handshake_send) >= Duration::from_millis(250)
    {
        let packet = peer.initial_packet.as_deref().unwrap_or_else(|| {
            static PUNCH: [u8; 5] = [b'P', b'2', b'P', b'2', PACKET_PUNCH];
            &PUNCH
        });
        udp.send_to(packet, peer.address).await?;
        peer.last_handshake_send = now;
    } else if peer.transport.is_some()
        && !peer.connected
        && now.duration_since(peer.last_ready_send) >= Duration::from_millis(500)
    {
        send_ready(peer, udp).await.map_err(network_error_to_io)?;
    }
    if peer.connected && now.duration_since(peer.last_received) > Duration::from_secs(90) {
        let _ = event_tx.send(NetworkEvent::PeerDisconnected).await;
        return Ok(true);
    }
    if peer.connected && now.duration_since(peer.last_heartbeat_send) >= Duration::from_secs(30) {
        let packet = peer
            .encrypt(&[PLAIN_HEARTBEAT])
            .map_err(network_error_to_io)?;
        udp.send_to(&packet, peer.address).await?;
        peer.last_heartbeat_send = now;
    }

    let due = peer
        .pending
        .iter()
        .filter(|(_, pending)| now >= pending.next_retry)
        .map(|(id, _)| *id)
        .collect::<Vec<_>>();
    for id in due {
        let Some(pending) = peer.pending.remove(&id) else {
            continue;
        };
        if pending.retries >= 4 {
            let _ = event_tx
                .send(NetworkEvent::MessageFailed {
                    id,
                    reason: "상대방이 메시지 수신을 확인하지 않았습니다.".to_owned(),
                })
                .await;
            continue;
        }
        let packet =
            encrypted_message_packet(peer, id, &pending.text).map_err(network_error_to_io)?;
        udp.send_to(&packet, peer.address).await?;
        let delays = [
            Duration::from_secs(1),
            Duration::from_secs(2),
            Duration::from_secs(4),
            Duration::from_secs(4),
        ];
        let retries = pending.retries + 1;
        peer.pending.insert(
            id,
            PendingMessage {
                text: pending.text,
                retries,
                next_retry: now + delays[retries.saturating_sub(1).min(3)],
            },
        );
    }
    Ok(false)
}

fn encrypted_message_packet(
    peer: &mut SecurePeer,
    id: u64,
    text: &str,
) -> Result<Vec<u8>, NetworkError> {
    let mut plaintext = Vec::with_capacity(9 + text.len());
    plaintext.push(PLAIN_MESSAGE);
    plaintext.extend_from_slice(&id.to_be_bytes());
    plaintext.extend_from_slice(text.as_bytes());
    peer.encrypt(&plaintext)
}

async fn fail_pending(
    peer: &mut Option<SecurePeer>,
    event_tx: &mpsc::Sender<NetworkEvent>,
    reason: &str,
) {
    let Some(peer) = peer.as_mut() else { return };
    for id in peer.pending.keys().copied().collect::<Vec<_>>() {
        let _ = event_tx
            .send(NetworkEvent::MessageFailed {
                id,
                reason: reason.to_owned(),
            })
            .await;
    }
    peer.pending.clear();
}

fn wrap_packet(kind: u8, payload: &[u8]) -> Vec<u8> {
    let mut packet = Vec::with_capacity(5 + payload.len());
    packet.extend_from_slice(MAGIC);
    packet.push(kind);
    packet.extend_from_slice(payload);
    packet
}

async fn connect_tls(host: &str, address: &str) -> io::Result<TlsStream<TcpStream>> {
    let roots = RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = ClientConfig::builder_with_protocol_versions(&[&TLS13])
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let tcp = timeout(Duration::from_secs(10), TcpStream::connect(address))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "connection timed out"))??;
    let server_name = ServerName::try_from(host.to_owned())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid TLS server name"))?;
    timeout(Duration::from_secs(10), connector.connect(server_name, tcp))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "TLS handshake timed out"))?
        .map_err(io::Error::other)
}

fn server_addresses() -> (String, String, String) {
    let host = env::var("P2P_SERVER_NAME").unwrap_or_else(|_| DEFAULT_SERVER_HOST.to_owned());
    let tcp = env::var("P2P_TCP_SERVER").unwrap_or_else(|_| format!("{host}:{DEFAULT_TCP_PORT}"));
    let udp = env::var("P2P_UDP_SERVER").unwrap_or_else(|_| format!("{host}:{DEFAULT_UDP_PORT}"));
    (host, tcp, udp)
}

fn parse_hello(line: &str) -> Option<(String, [u8; 32])> {
    let mut parts = line.split_whitespace();
    match (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ) {
        (Some("HELLO"), Some(version), Some(client_id), Some(secret), None)
            if version.parse::<u8>().ok() == Some(PROTOCOL_VERSION)
                && decode_hex::<16>(client_id).is_some() =>
        {
            Some((client_id.to_owned(), decode_hex::<32>(secret)?))
        }
        _ => None,
    }
}

fn registration_payload(
    client_id: &str,
    client_nonce: &[u8; 16],
    server_nonce: &[u8; 16],
    source: SocketAddr,
) -> String {
    format!(
        "REG2|{client_id}|{}|{}|{source}",
        encode_hex(client_nonce),
        encode_hex(server_nonce)
    )
}

async fn read_bounded_line<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<Option<String>> {
    let mut bytes = Vec::with_capacity(128);
    loop {
        let mut byte = [0_u8; 1];
        let count = reader.read(&mut byte).await?;
        if count == 0 {
            return if bytes.is_empty() {
                Ok(None)
            } else {
                Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "partial signaling line",
                ))
            };
        }
        if byte[0] == b'\n' {
            break;
        }
        if byte[0] != b'\r' {
            bytes.push(byte[0]);
        }
        if bytes.len() > MAX_SIGNAL_LINE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "signaling line too long",
            ));
        }
    }
    String::from_utf8(bytes)
        .map(Some)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "signaling line is not UTF-8"))
}

async fn send_protocol_error(event_tx: &mpsc::Sender<NetworkEvent>) {
    let _ = event_tx
        .send(NetworkEvent::Error(NetworkError::ProtocolMismatch))
        .await;
}

fn random_bytes<const N: usize>() -> [u8; N] {
    let mut bytes = [0_u8; N];
    rand::rng().fill_bytes(&mut bytes);
    bytes
}

fn safe_io_error(error: &io::Error) -> String {
    match error.kind() {
        io::ErrorKind::TimedOut => "연결 시간이 초과되었습니다.".to_owned(),
        io::ErrorKind::ConnectionRefused => "서버가 연결을 거부했습니다.".to_owned(),
        io::ErrorKind::ConnectionReset
        | io::ErrorKind::BrokenPipe
        | io::ErrorKind::UnexpectedEof => "연결이 예기치 않게 종료되었습니다.".to_owned(),
        _ => error.to_string(),
    }
}

fn network_error_to_io(error: NetworkError) -> io::Error {
    io::Error::other(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_window_rejects_duplicates_and_old_packets() {
        let mut window = ReplayWindow::default();
        assert!(window.would_accept(10));
        window.record(10);
        assert!(!window.would_accept(10));
        assert!(window.would_accept(9));
        window.record(200);
        assert!(!window.would_accept(10));
    }

    #[test]
    fn noise_psk_authenticates_and_detects_tampering() {
        let invite = InviteCode::generate();
        let room = RoomContext {
            invite: invite.clone(),
            room_id: invite.room_id(),
            session_nonce: [9; 16],
        };
        let address: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let mut initiator = SecurePeer::new(address, true, &room).unwrap();
        let mut responder = SecurePeer::new(address, false, &room).unwrap();
        let init = initiator.initial_packet.as_ref().unwrap();
        let mut scratch = [0_u8; MAX_DATAGRAM];
        let mut responder_hs = responder.handshake.take().unwrap();
        responder_hs.read_message(&init[5..], &mut scratch).unwrap();
        let mut response = [0_u8; MAX_DATAGRAM];
        let size = responder_hs.write_message(&[], &mut response).unwrap();
        responder.transport = Some(responder_hs.into_stateless_transport_mode().unwrap());
        let mut initiator_hs = initiator.handshake.take().unwrap();
        initiator_hs
            .read_message(&response[..size], &mut scratch)
            .unwrap();
        initiator.transport = Some(initiator_hs.into_stateless_transport_mode().unwrap());
        let packet = initiator.encrypt(b"secret").unwrap();
        assert_eq!(responder.decrypt(&packet).unwrap().unwrap(), b"secret");

        let mut tampered = initiator.encrypt(b"tamper me").unwrap();
        *tampered.last_mut().unwrap() ^= 1;
        assert!(responder.decrypt(&tampered).unwrap().is_none());
    }

    #[test]
    fn wrong_invite_cannot_complete_noise_handshake() {
        let invite_a = InviteCode::generate();
        let invite_b = InviteCode::generate();
        let room_a = RoomContext {
            invite: invite_a.clone(),
            room_id: invite_a.room_id(),
            session_nonce: [4; 16],
        };
        let room_b = RoomContext {
            invite: invite_b.clone(),
            room_id: invite_a.room_id(),
            session_nonce: [4; 16],
        };
        let address: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let initiator = SecurePeer::new(address, true, &room_a).unwrap();
        let mut responder = SecurePeer::new(address, false, &room_b).unwrap();
        let mut scratch = [0_u8; MAX_DATAGRAM];
        assert!(
            responder
                .handshake
                .as_mut()
                .unwrap()
                .read_message(&initiator.initial_packet.unwrap()[5..], &mut scratch)
                .is_err()
        );
    }
}

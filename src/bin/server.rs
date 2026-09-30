use hmac::{Hmac, Mac};
use p2p_chat::protocol::{
    MAX_DATAGRAM, MAX_SIGNAL_LINE, PROTOCOL_VERSION, decode_hex, decode_public_room_title,
    encode_hex, encode_public_room_title,
};
use rand::RngCore;
use sha2::Sha256;
use socket2::{Domain, Protocol, Socket, Type};
use std::{
    collections::HashMap,
    env, io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
    sync::{Mutex, Semaphore, mpsc},
    task::JoinSet,
    time::{interval, sleep, timeout},
};
use tokio_rustls::{
    TlsAcceptor,
    rustls::{
        self, ServerConfig,
        pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
    },
};
use tracing::{info, warn};

type ClientSender = mpsc::Sender<String>;
type HmacSha256 = Hmac<Sha256>;

const TCP_BIND: &str = "0.0.0.0:9000";
const UDP_BIND: &str = "0.0.0.0:9001";
const TLS_TIMEOUT: Duration = Duration::from_secs(10);
const ROOM_TTL: Duration = Duration::from_secs(10 * 60);
const REGISTRATION_TTL: Duration = Duration::from_secs(5);
const PEER_MATCH_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_CONNECTIONS: usize = 1024;
const MAX_CONNECTIONS_PER_IP: usize = 20;
const MAX_PUBLIC_ROOMS: usize = 100;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum IpFamily {
    V4,
    V6,
}

impl IpFamily {
    fn of(address: SocketAddr) -> Self {
        if address.is_ipv4() {
            Self::V4
        } else {
            Self::V6
        }
    }

    fn protocol_token(self) -> &'static str {
        match self {
            Self::V4 => "4",
            Self::V6 => "6",
        }
    }
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum IpRateKey {
    V4(Ipv4Addr),
    V6Prefix64([u8; 8]),
}

#[derive(Clone, Default)]
struct PeerEndpoints {
    ipv4: Option<SocketAddr>,
    ipv6: Option<SocketAddr>,
}

impl PeerEndpoints {
    fn insert(&mut self, address: SocketAddr) {
        match IpFamily::of(address) {
            IpFamily::V4 => self.ipv4 = Some(address),
            IpFamily::V6 => self.ipv6 = Some(address),
        }
    }

    fn get(&self, family: IpFamily) -> Option<SocketAddr> {
        match family {
            IpFamily::V4 => self.ipv4,
            IpFamily::V6 => self.ipv6,
        }
    }

    fn is_empty(&self) -> bool {
        self.ipv4.is_none() && self.ipv6.is_none()
    }
}

#[derive(Clone)]
struct Peer {
    sender: ClientSender,
    endpoints: PeerEndpoints,
}

struct Room {
    creator: String,
    kind: RoomKind,
    session_nonce: [u8; 16],
    created_at: Instant,
    peers: HashMap<String, Peer>,
    selected_family: Option<IpFamily>,
}

enum RoomKind {
    Private,
    Public { title: String, list_rank: u64 },
}

impl RoomKind {
    fn is_public(&self) -> bool {
        matches!(self, Self::Public { .. })
    }
}

struct RegistrationChallenge {
    source: SocketAddr,
    client_nonce: [u8; 16],
    server_nonce: [u8; 16],
    expires_at: Instant,
}

struct ClientRecord {
    sender: ClientSender,
    registration_secret: [u8; 32],
    challenges: HashMap<IpFamily, RegistrationChallenge>,
    supports_dual_stack: bool,
}

impl Drop for ClientRecord {
    fn drop(&mut self) {
        use zeroize::Zeroize;
        self.registration_secret.zeroize();
    }
}

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
enum RateClass {
    General,
    Room,
    Udp,
}

struct TokenBucket {
    tokens: f64,
    updated_at: Instant,
}

#[derive(Default)]
struct ServerState {
    rooms: HashMap<String, Room>,
    clients: HashMap<String, ClientRecord>,
    rates: HashMap<(IpRateKey, RateClass), TokenBucket>,
    next_public_rank: u64,
}

type SharedState = Arc<Mutex<ServerState>>;

#[tokio::main]
async fn main() -> io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .compact()
        .init();

    let cert_path = required_env("P2P_TLS_CERT_PATH")?;
    let key_path = required_env("P2P_TLS_KEY_PATH")?;
    let tls = TlsAcceptor::from(Arc::new(load_tls_config(&cert_path, &key_path)?));
    let tcp_bind = env::var("P2P_TCP_BIND").unwrap_or_else(|_| TCP_BIND.to_owned());
    let udp_bind = env::var("P2P_UDP_BIND").unwrap_or_else(|_| UDP_BIND.to_owned());
    let tcp_bind_v6 = optional_env("P2P_TCP_BIND_V6");
    let udp_bind_v6 = optional_env("P2P_UDP_BIND_V6");
    let state = Arc::new(Mutex::new(ServerState::default()));
    let connection_limit = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    let connection_counts = Arc::new(Mutex::new(HashMap::<IpRateKey, usize>::new()));

    let mut udp_binds = vec![udp_bind.clone()];
    if let Some(address) = udp_bind_v6 {
        udp_binds.push(address);
    }
    for address in &udp_binds {
        let socket = bind_udp_listener(address)?;
        tokio::spawn(handle_udp_registration(socket, Arc::clone(&state)));
    }

    tokio::spawn(cleanup_expired_rooms(Arc::clone(&state)));

    let mut tcp_binds = vec![tcp_bind.clone()];
    if let Some(address) = tcp_bind_v6 {
        tcp_binds.push(address);
    }
    let mut listeners = JoinSet::new();
    for address in &tcp_binds {
        let listener = bind_tcp_listener(address)?;
        listeners.spawn(accept_connections(
            listener,
            tls.clone(),
            Arc::clone(&state),
            Arc::clone(&connection_limit),
            Arc::clone(&connection_counts),
        ));
    }

    info!(version = PROTOCOL_VERSION, tcp = ?tcp_binds, udp = ?udp_binds, "signaling server started");

    match listeners.join_next().await {
        Some(Ok(result)) => result,
        Some(Err(error)) => Err(io::Error::other(format!(
            "TCP listener task failed: {error}"
        ))),
        None => Err(io::Error::other("no TCP listeners were started")),
    }
}

async fn accept_connections(
    tcp_listener: TcpListener,
    tls: TlsAcceptor,
    state: SharedState,
    connection_limit: Arc<Semaphore>,
    connection_counts: Arc<Mutex<HashMap<IpRateKey, usize>>>,
) -> io::Result<()> {
    loop {
        let (stream, address) = tcp_listener.accept().await?;
        let Ok(permit) = Arc::clone(&connection_limit).try_acquire_owned() else {
            warn!("connection limit reached");
            continue;
        };

        let rate_key = ip_rate_key(address.ip());
        {
            let mut counts = connection_counts.lock().await;
            let count = counts.entry(rate_key).or_default();
            if *count >= MAX_CONNECTIONS_PER_IP {
                warn!("per-IP connection limit reached");
                continue;
            }
            *count += 1;
        }

        let tls = tls.clone();
        let state = Arc::clone(&state);
        let counts = Arc::clone(&connection_counts);
        tokio::spawn(async move {
            let _permit = permit;
            let result = timeout(TLS_TIMEOUT, tls.accept(stream)).await;
            match result {
                Ok(Ok(stream)) => {
                    if let Err(error) = handle_client(stream, address, state).await {
                        warn!(error = %error, "client session ended");
                    }
                }
                Ok(Err(error)) => warn!(error = %error, "TLS handshake rejected"),
                Err(_) => warn!("TLS handshake timed out"),
            }

            let mut counts = counts.lock().await;
            if let Some(count) = counts.get_mut(&rate_key) {
                *count -= 1;
                if *count == 0 {
                    counts.remove(&rate_key);
                }
            }
        });
    }
}

fn required_env(name: &str) -> io::Result<String> {
    env::var(name)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, format!("{name} is required")))
}

fn optional_env(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.trim().is_empty())
}

fn bind_tcp_listener(address: &str) -> io::Result<TcpListener> {
    let address = address.parse::<SocketAddr>().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid TCP bind address '{address}': {error}"),
        )
    })?;
    let socket = new_bound_socket(address, Type::STREAM, Protocol::TCP)?;
    socket.listen(1024)?;
    let listener: std::net::TcpListener = socket.into();
    listener.set_nonblocking(true)?;
    TcpListener::from_std(listener)
}

fn bind_udp_listener(address: &str) -> io::Result<UdpSocket> {
    let address = address.parse::<SocketAddr>().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid UDP bind address '{address}': {error}"),
        )
    })?;
    let socket = new_bound_socket(address, Type::DGRAM, Protocol::UDP)?;
    let socket: std::net::UdpSocket = socket.into();
    socket.set_nonblocking(true)?;
    UdpSocket::from_std(socket)
}

fn new_bound_socket(
    address: SocketAddr,
    socket_type: Type,
    protocol: Protocol,
) -> io::Result<Socket> {
    let domain = if address.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let socket = Socket::new(domain, socket_type, Some(protocol))?;
    socket.set_reuse_address(true)?;
    if address.is_ipv6() {
        socket.set_only_v6(true)?;
    }
    socket.bind(&address.into())?;
    Ok(socket)
}

fn load_tls_config(cert_path: &str, key_path: &str) -> io::Result<ServerConfig> {
    let certificates: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(cert_path)
        .map_err(pem_error)?
        .collect::<Result<_, _>>()
        .map_err(pem_error)?;
    if certificates.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "certificate chain is empty",
        ));
    }
    let key = PrivateKeyDer::from_pem_file(key_path).map_err(pem_error)?;

    ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
        .with_no_client_auth()
        .with_single_cert(certificates, key)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))
}

fn pem_error(error: rustls::pki_types::pem::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

async fn handle_client(
    stream: tokio_rustls::server::TlsStream<TcpStream>,
    tcp_addr: SocketAddr,
    state: SharedState,
) -> io::Result<()> {
    let client_id = random_hex::<16>();
    let registration_secret = random_bytes::<32>();
    let (mut reader, writer) = tokio::io::split(stream);
    let (tx, rx) = mpsc::channel::<String>(32);
    tokio::spawn(write_messages(writer, rx));

    {
        let mut state = state.lock().await;
        state.clients.insert(
            client_id.clone(),
            ClientRecord {
                sender: tx.clone(),
                registration_secret,
                challenges: HashMap::new(),
                supports_dual_stack: false,
            },
        );
    }

    tx.send(format!(
        "HELLO {PROTOCOL_VERSION} {client_id} {}\n",
        encode_hex(&registration_secret)
    ))
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "client writer closed"))?;

    let mut current_room: Option<String> = None;
    let mut missed_heartbeats = 0_u8;
    let mut line_buffer = Vec::with_capacity(128);

    loop {
        let line = match timeout(
            Duration::from_secs(30),
            read_bounded_line(&mut reader, &mut line_buffer),
        )
        .await
        {
            Ok(result) => result?,
            Err(_) => {
                missed_heartbeats += 1;
                if missed_heartbeats >= 3 || tx.send("PING\n".to_owned()).await.is_err() {
                    break;
                }
                continue;
            }
        };
        let Some(line) = line else { break };
        missed_heartbeats = 0;

        if !rate_allow(&state, tcp_addr.ip(), RateClass::General).await {
            let _ = tx.send("RATE_LIMITED\n".to_owned()).await;
            continue;
        }

        let mut parts = line.split_whitespace();
        match parts.next() {
            Some("PONG") => {}
            Some("CREATE") => {
                if current_room.is_some() {
                    let _ = tx.send("ALREADY_IN_ROOM\n".to_owned()).await;
                    continue;
                }
                if !rate_allow(&state, tcp_addr.ip(), RateClass::Room).await {
                    let _ = tx.send("RATE_LIMITED\n".to_owned()).await;
                    continue;
                }
                let Some(room_id) = parts.next().filter(|id| decode_hex::<16>(id).is_some()) else {
                    let _ = tx.send("ERROR INVALID_ROOM_ID\n".to_owned()).await;
                    continue;
                };
                if parts.next().is_some() {
                    let _ = tx.send("ERROR INVALID_COMMAND\n".to_owned()).await;
                    continue;
                }
                match create_room(&state, room_id, &client_id, tx.clone()).await {
                    Ok(()) => current_room = Some(room_id.to_owned()),
                    Err(code) => {
                        let _ = tx.send(format!("{code}\n")).await;
                    }
                }
            }
            Some("CREATE_PUBLIC") => {
                if current_room.is_some() {
                    let _ = tx.send("ALREADY_IN_ROOM\n".to_owned()).await;
                    continue;
                }
                if !rate_allow(&state, tcp_addr.ip(), RateClass::Room).await {
                    let _ = tx.send("RATE_LIMITED\n".to_owned()).await;
                    continue;
                }
                let (Some(room_id), Some(encoded_title)) = (parts.next(), parts.next()) else {
                    let _ = tx.send("ERROR INVALID_COMMAND\n".to_owned()).await;
                    continue;
                };
                let Some(title) = decode_public_room_title(encoded_title)
                    .filter(|_| decode_hex::<16>(room_id).is_some() && parts.next().is_none())
                else {
                    let _ = tx.send("ERROR INVALID_PUBLIC_ROOM\n".to_owned()).await;
                    continue;
                };
                match create_public_room(&state, room_id, title, &client_id, tx.clone()).await {
                    Ok(()) => current_room = Some(room_id.to_owned()),
                    Err(code) => {
                        let _ = tx.send(format!("{code}\n")).await;
                    }
                }
            }
            Some("JOIN") => {
                if current_room.is_some() {
                    let _ = tx.send("ALREADY_IN_ROOM\n".to_owned()).await;
                    continue;
                }
                if !rate_allow(&state, tcp_addr.ip(), RateClass::Room).await {
                    let _ = tx.send("RATE_LIMITED\n".to_owned()).await;
                    continue;
                }
                let Some(room_id) = parts.next().filter(|id| decode_hex::<16>(id).is_some()) else {
                    let _ = tx.send("ERROR INVALID_ROOM_ID\n".to_owned()).await;
                    continue;
                };
                match join_room(&state, room_id, &client_id, tx.clone()).await {
                    Ok(()) => current_room = Some(room_id.to_owned()),
                    Err(code) => {
                        let _ = tx.send(format!("{code}\n")).await;
                    }
                }
            }
            Some("JOIN_PUBLIC") => {
                if current_room.is_some() {
                    let _ = tx.send("ALREADY_IN_ROOM\n".to_owned()).await;
                    continue;
                }
                if !rate_allow(&state, tcp_addr.ip(), RateClass::Room).await {
                    let _ = tx.send("RATE_LIMITED\n".to_owned()).await;
                    continue;
                }
                let Some(room_id) = parts
                    .next()
                    .filter(|id| decode_hex::<16>(id).is_some() && parts.next().is_none())
                else {
                    let _ = tx.send("ERROR INVALID_ROOM_ID\n".to_owned()).await;
                    continue;
                };
                match join_public_room(&state, room_id, &client_id, tx.clone()).await {
                    Ok(()) => current_room = Some(room_id.to_owned()),
                    Err(code) => {
                        let _ = tx.send(format!("{code}\n")).await;
                    }
                }
            }
            Some("LIST_PUBLIC") => {
                if parts.next().is_some() {
                    let _ = tx.send("ERROR INVALID_COMMAND\n".to_owned()).await;
                    continue;
                }
                send_public_rooms(&state, &tx).await;
            }
            Some("LEAVE") | Some("P2P_FAILED") => {
                if let Some(room_id) = current_room.take() {
                    leave_room(&state, &room_id, &client_id).await;
                }
                let _ = tx.send("LEFT\n".to_owned()).await;
            }
            _ => {
                let _ = tx.send("ERROR UNKNOWN_COMMAND\n".to_owned()).await;
            }
        }
    }

    if let Some(room_id) = current_room {
        leave_room(&state, &room_id, &client_id).await;
    }
    state.lock().await.clients.remove(&client_id);
    Ok(())
}

async fn create_room(
    state: &SharedState,
    room_id: &str,
    client_id: &str,
    sender: ClientSender,
) -> Result<(), &'static str> {
    let nonce = random_bytes::<16>();
    {
        let mut state = state.lock().await;
        if state.rooms.contains_key(room_id) {
            return Err("ROOM_EXISTS");
        }
        let mut peers = HashMap::new();
        peers.insert(
            client_id.to_owned(),
            Peer {
                sender: sender.clone(),
                endpoints: PeerEndpoints::default(),
            },
        );
        state.rooms.insert(
            room_id.to_owned(),
            Room {
                creator: client_id.to_owned(),
                kind: RoomKind::Private,
                session_nonce: nonce,
                created_at: Instant::now(),
                peers,
                selected_family: None,
            },
        );
    }
    let _ = sender
        .send(format!("CREATED {room_id} {}\n", encode_hex(&nonce)))
        .await;
    info!(room = %short_id(room_id), "room created");
    Ok(())
}

async fn create_public_room(
    state: &SharedState,
    room_id: &str,
    title: String,
    client_id: &str,
    sender: ClientSender,
) -> Result<(), &'static str> {
    let nonce = random_bytes::<16>();
    let rank = {
        let mut state = state.lock().await;
        if state.rooms.contains_key(room_id) {
            return Err("ROOM_EXISTS");
        }
        if state
            .rooms
            .values()
            .filter(|room| room.kind.is_public())
            .count()
            >= MAX_PUBLIC_ROOMS
        {
            return Err("PUBLIC_ROOM_LIMIT");
        }
        state.next_public_rank = state.next_public_rank.wrapping_add(1).max(1);
        let rank = state.next_public_rank;
        let peers = HashMap::from([(
            client_id.to_owned(),
            Peer {
                sender: sender.clone(),
                endpoints: PeerEndpoints::default(),
            },
        )]);
        state.rooms.insert(
            room_id.to_owned(),
            Room {
                creator: client_id.to_owned(),
                kind: RoomKind::Public {
                    title: title.clone(),
                    list_rank: rank,
                },
                session_nonce: nonce,
                created_at: Instant::now(),
                peers,
                selected_family: None,
            },
        );
        rank
    };
    let encoded_title = encode_public_room_title(&title).expect("validated public room title");
    let _ = sender
        .send(format!(
            "PUBLIC_CREATED {room_id} {} {encoded_title}\n",
            encode_hex(&nonce)
        ))
        .await;
    info!(room = %short_id(room_id), rank, "public room created");
    Ok(())
}

async fn join_room(
    state: &SharedState,
    room_id: &str,
    client_id: &str,
    sender: ClientSender,
) -> Result<(), &'static str> {
    join_room_kind(state, room_id, client_id, sender, false).await
}

async fn join_public_room(
    state: &SharedState,
    room_id: &str,
    client_id: &str,
    sender: ClientSender,
) -> Result<(), &'static str> {
    join_room_kind(state, room_id, client_id, sender, true).await
}

async fn join_room_kind(
    state: &SharedState,
    room_id: &str,
    client_id: &str,
    sender: ClientSender,
    public: bool,
) -> Result<(), &'static str> {
    let (nonce, public_title) = {
        let mut state = state.lock().await;
        let room = state.rooms.get_mut(room_id).ok_or("ROOM_NOT_FOUND")?;
        if room.kind.is_public() != public {
            return Err("ROOM_NOT_FOUND");
        }
        if room.peers.len() >= 2 {
            return Err("ROOM_FULL");
        }
        room.peers.insert(
            client_id.to_owned(),
            Peer {
                sender: sender.clone(),
                endpoints: PeerEndpoints::default(),
            },
        );
        let title = match &room.kind {
            RoomKind::Private => None,
            RoomKind::Public { title, .. } => Some(title.clone()),
        };
        (room.session_nonce, title)
    };
    let response = if let Some(title) = public_title {
        let title = encode_public_room_title(&title).expect("validated public room title");
        format!("PUBLIC_JOINED {room_id} {} {title}\n", encode_hex(&nonce))
    } else {
        format!("JOINED {room_id} {}\n", encode_hex(&nonce))
    };
    let _ = sender.send(response).await;
    schedule_peer_match_timeout(Arc::clone(state), room_id.to_owned(), nonce);
    info!(room = %short_id(room_id), "room joined");
    Ok(())
}

async fn leave_room(state: &SharedState, room_id: &str, client_id: &str) {
    let senders = {
        let mut state = state.lock().await;
        let reopen_public = state.rooms.get(room_id).is_some_and(|room| {
            room.kind.is_public() && room.peers.len() == 2 && room.peers.contains_key(client_id)
        });
        let new_rank = if reopen_public {
            state.next_public_rank = state.next_public_rank.wrapping_add(1).max(1);
            Some(state.next_public_rank)
        } else {
            None
        };
        let Some(room) = state.rooms.get_mut(room_id) else {
            return;
        };
        let departing_creator = room.creator == client_id;
        room.peers.remove(client_id);
        room.selected_family = None;
        if let Some(remaining_client_id) = room.peers.keys().next().cloned() {
            if departing_creator {
                room.creator = remaining_client_id;
            }
            room.created_at = Instant::now();
        }
        if let (Some(rank), RoomKind::Public { list_rank, .. }) = (new_rank, &mut room.kind) {
            *list_rank = rank;
        }
        let senders = room
            .peers
            .values()
            .map(|peer| peer.sender.clone())
            .collect::<Vec<_>>();
        if room.peers.is_empty() {
            state.rooms.remove(room_id);
        }
        senders
    };
    for sender in senders {
        let _ = sender.send("PEER_LEFT\n".to_owned()).await;
    }
    info!(room = %short_id(room_id), "room left");
}

async fn send_public_rooms(state: &SharedState, sender: &ClientSender) {
    let mut rooms = {
        let state = state.lock().await;
        state
            .rooms
            .iter()
            .filter_map(|(room_id, room)| match &room.kind {
                RoomKind::Private => None,
                RoomKind::Public { title, list_rank } => Some((
                    room_id.clone(),
                    title.clone(),
                    room.peers.len().min(2) as u8,
                    *list_rank,
                )),
            })
            .collect::<Vec<_>>()
    };
    rooms.sort_by(|left, right| {
        let left_full = left.2 >= 2;
        let right_full = right.2 >= 2;
        left_full
            .cmp(&right_full)
            .then_with(|| right.3.cmp(&left.3))
    });

    if sender.send("PUBLIC_LIST_BEGIN\n".to_owned()).await.is_err() {
        return;
    }
    for (room_id, title, occupants, _) in rooms {
        let title = encode_public_room_title(&title).expect("validated public room title");
        if sender
            .send(format!("PUBLIC_ROOM {room_id} {occupants} {title}\n"))
            .await
            .is_err()
        {
            return;
        }
    }
    let _ = sender.send("PUBLIC_LIST_END\n".to_owned()).await;
}

fn schedule_peer_match_timeout(state: SharedState, room_id: String, session_nonce: [u8; 16]) {
    tokio::spawn(async move {
        sleep(PEER_MATCH_TIMEOUT).await;
        let senders = {
            let state = state.lock().await;
            let Some(room) = state.rooms.get(&room_id) else {
                return;
            };
            if room.session_nonce != session_nonce
                || room.peers.len() != 2
                || room.selected_family.is_some()
                || room.peers.values().any(|peer| peer.endpoints.is_empty())
            {
                return;
            }
            room.peers
                .keys()
                .filter_map(|client_id| state.clients.get(client_id))
                .filter(|client| client.supports_dual_stack)
                .map(|client| client.sender.clone())
                .collect::<Vec<_>>()
        };
        for sender in senders {
            let _ = sender
                .send("P2P_UNAVAILABLE ADDRESS_FAMILY\n".to_owned())
                .await;
        }
    });
}

async fn handle_udp_registration(socket: UdpSocket, state: SharedState) {
    let mut buffer = [0_u8; MAX_DATAGRAM + 1];
    loop {
        let Ok((size, source)) = socket.recv_from(&mut buffer).await else {
            continue;
        };
        if size > MAX_DATAGRAM || !rate_allow(&state, source.ip(), RateClass::Udp).await {
            continue;
        }
        let Ok(message) = std::str::from_utf8(&buffer[..size]) else {
            continue;
        };
        let mut parts = message.split_whitespace();
        match parts.next() {
            Some("PROBE") => {
                let (Some(client_id), Some(client_nonce)) = (parts.next(), parts.next()) else {
                    continue;
                };
                let Some(client_nonce) = decode_hex::<16>(client_nonce) else {
                    continue;
                };
                let supports_dual_stack = parts.next() == Some("DS");
                let server_nonce = random_bytes::<16>();
                let sender = {
                    let mut state = state.lock().await;
                    let Some(client) = state.clients.get_mut(client_id) else {
                        continue;
                    };
                    client.supports_dual_stack |= supports_dual_stack;
                    client.challenges.insert(
                        IpFamily::of(source),
                        RegistrationChallenge {
                            source,
                            client_nonce,
                            server_nonce,
                            expires_at: Instant::now() + REGISTRATION_TTL,
                        },
                    );
                    client.sender.clone()
                };
                let _ = sender
                    .send(format!(
                        "UDP_CHALLENGE {source} {} {}\n",
                        encode_hex(&client_nonce),
                        encode_hex(&server_nonce)
                    ))
                    .await;
            }
            Some("REGISTER") => {
                let (Some(client_id), Some(client_nonce), Some(server_nonce), Some(tag)) =
                    (parts.next(), parts.next(), parts.next(), parts.next())
                else {
                    continue;
                };
                let (Some(client_nonce), Some(server_nonce), Some(tag)) = (
                    decode_hex::<16>(client_nonce),
                    decode_hex::<16>(server_nonce),
                    decode_hex::<32>(tag),
                ) else {
                    continue;
                };
                if authenticate_registration(
                    &state,
                    client_id,
                    source,
                    client_nonce,
                    server_nonce,
                    tag,
                )
                .await
                {
                    register_udp_addr(&state, client_id, source).await;
                }
            }
            _ => {}
        }
    }
}

async fn authenticate_registration(
    state: &SharedState,
    client_id: &str,
    source: SocketAddr,
    client_nonce: [u8; 16],
    server_nonce: [u8; 16],
    received_tag: [u8; 32],
) -> bool {
    let mut state = state.lock().await;
    let Some(client) = state.clients.get_mut(client_id) else {
        return false;
    };
    let Some(challenge) = client.challenges.remove(&IpFamily::of(source)) else {
        return false;
    };
    if challenge.source != source
        || challenge.client_nonce != client_nonce
        || challenge.server_nonce != server_nonce
        || challenge.expires_at < Instant::now()
    {
        return false;
    }
    let payload = registration_payload(client_id, &client_nonce, &server_nonce, source);
    let mut mac = HmacSha256::new_from_slice(&client.registration_secret).expect("HMAC key length");
    mac.update(payload.as_bytes());
    mac.verify_slice(&received_tag).is_ok()
}

async fn register_udp_addr(state: &SharedState, client_id: &str, udp_addr: SocketAddr) {
    let (registered, pair) = {
        let mut state = state.lock().await;
        let registered = state
            .clients
            .get(client_id)
            .map(|client| client.sender.clone());
        let mut pair = None;
        for (room_id, room) in &mut state.rooms {
            let Some(peer) = room.peers.get_mut(client_id) else {
                continue;
            };
            peer.endpoints.insert(udp_addr);
            if room.peers.len() == 2 && room.selected_family.is_none() {
                let family = select_common_family(room.peers.values());
                if let Some(family) = family {
                    room.selected_family = Some(family);
                    let peers = room
                        .peers
                        .iter()
                        .map(|(id, peer)| {
                            (
                                id.clone(),
                                peer.sender.clone(),
                                peer.endpoints.get(family).expect("common family checked"),
                            )
                        })
                        .collect::<Vec<_>>();
                    pair = Some((
                        room_id.clone(),
                        room.creator.clone(),
                        room.session_nonce,
                        family,
                        peers,
                    ));
                }
            }
            break;
        }
        (registered, pair)
    };
    if let Some(sender) = registered {
        let _ = sender
            .send(format!(
                "REGISTERED {}\n",
                IpFamily::of(udp_addr).protocol_token()
            ))
            .await;
    }
    if let Some((room_id, creator, nonce, family, peers)) = pair {
        for (id, sender, _) in &peers {
            let other = peers
                .iter()
                .find(|(other_id, _, _)| other_id != id)
                .expect("two peers");
            let role = if *id == creator { "I" } else { "R" };
            let _ = sender
                .send(format!("PEER {} {role} {}\n", other.2, encode_hex(&nonce)))
                .await;
        }
        info!(room = %short_id(&room_id), ?family, "peer endpoints exchanged");
    }
}

fn select_common_family<'a>(peers: impl Iterator<Item = &'a Peer>) -> Option<IpFamily> {
    let peers = peers.collect::<Vec<_>>();
    [IpFamily::V6, IpFamily::V4].into_iter().find(|family| {
        peers
            .iter()
            .all(|peer| peer.endpoints.get(*family).is_some())
    })
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

async fn rate_allow(state: &SharedState, ip: IpAddr, class: RateClass) -> bool {
    let (capacity, refill_per_second) = match class {
        RateClass::General => (20.0, 2.0),
        RateClass::Room => (10.0, 10.0 / 60.0),
        RateClass::Udp => (30.0, 30.0 / 60.0),
    };
    let mut state = state.lock().await;
    let now = Instant::now();
    let bucket = state
        .rates
        .entry((ip_rate_key(ip), class))
        .or_insert(TokenBucket {
            tokens: capacity,
            updated_at: now,
        });
    bucket.tokens = (bucket.tokens
        + now.duration_since(bucket.updated_at).as_secs_f64() * refill_per_second)
        .min(capacity);
    bucket.updated_at = now;
    if bucket.tokens < 1.0 {
        return false;
    }
    bucket.tokens -= 1.0;
    true
}

fn ip_rate_key(ip: IpAddr) -> IpRateKey {
    match ip {
        IpAddr::V4(address) => IpRateKey::V4(address),
        IpAddr::V6(address) => {
            if let Some(mapped) = address.to_ipv4_mapped() {
                IpRateKey::V4(mapped)
            } else {
                let octets = address.octets();
                IpRateKey::V6Prefix64(octets[..8].try_into().expect("fixed prefix length"))
            }
        }
    }
}

async fn cleanup_expired_rooms(state: SharedState) {
    let mut timer = interval(Duration::from_secs(60));
    loop {
        timer.tick().await;
        let expired = {
            let mut state = state.lock().await;
            let now = Instant::now();
            let ids = state
                .rooms
                .iter()
                .filter(|(_, room)| {
                    room.peers.len() < 2 && now.duration_since(room.created_at) >= ROOM_TTL
                })
                .map(|(id, _)| id.clone())
                .collect::<Vec<_>>();
            let mut senders = Vec::new();
            for id in ids {
                if let Some(room) = state.rooms.remove(&id) {
                    senders.extend(room.peers.into_values().map(|peer| peer.sender));
                }
            }
            if state.rates.len() > 10_000 {
                state.rates.retain(|_, bucket| {
                    now.duration_since(bucket.updated_at) < Duration::from_secs(3600)
                });
            }
            senders
        };
        for sender in expired {
            let _ = sender.send("ROOM_EXPIRED\n".to_owned()).await;
        }
    }
}

async fn read_bounded_line<R: AsyncRead + Unpin>(
    reader: &mut R,
    bytes: &mut Vec<u8>,
) -> io::Result<Option<String>> {
    loop {
        let mut byte = [0_u8; 1];
        let count = reader.read(&mut byte).await?;
        if count == 0 {
            return if bytes.is_empty() {
                Ok(None)
            } else {
                Err(io::Error::new(io::ErrorKind::UnexpectedEof, "partial line"))
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
    let line = std::mem::take(bytes);
    String::from_utf8(line)
        .map(Some)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "signaling line is not UTF-8"))
}

async fn write_messages<W: AsyncWrite + Unpin>(
    mut writer: W,
    mut receiver: mpsc::Receiver<String>,
) {
    while let Some(message) = receiver.recv().await {
        if writer.write_all(message.as_bytes()).await.is_err() {
            break;
        }
    }
}

fn random_bytes<const N: usize>() -> [u8; N] {
    let mut bytes = [0_u8; N];
    rand::rng().fill_bytes(&mut bytes);
    bytes
}

fn random_hex<const N: usize>() -> String {
    encode_hex(&random_bytes::<N>())
}
fn short_id(room_id: &str) -> &str {
    room_id.get(..6).unwrap_or("invalid")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[test]
    fn registration_payload_binds_address_and_nonces() {
        let id = "001122";
        let a = registration_payload(id, &[1; 16], &[2; 16], "127.0.0.1:9".parse().unwrap());
        let b = registration_payload(id, &[1; 16], &[2; 16], "127.0.0.1:10".parse().unwrap());
        assert_ne!(a, b);
    }

    #[test]
    fn registration_payload_supports_ipv6_addresses() {
        let source: SocketAddr = "[2001:db8::10]:40000".parse().unwrap();
        let payload = registration_payload("001122", &[1; 16], &[2; 16], source);
        assert!(payload.ends_with("|[2001:db8::10]:40000"));
    }

    #[test]
    fn common_family_prefers_ipv6_and_falls_back_to_ipv4() {
        let (sender_a, _receiver_a) = mpsc::channel(1);
        let (sender_b, _receiver_b) = mpsc::channel(1);
        let mut first = Peer {
            sender: sender_a,
            endpoints: PeerEndpoints::default(),
        };
        let mut second = Peer {
            sender: sender_b,
            endpoints: PeerEndpoints::default(),
        };
        first
            .endpoints
            .insert("198.51.100.1:40000".parse().unwrap());
        second
            .endpoints
            .insert("198.51.100.2:40001".parse().unwrap());
        assert_eq!(
            select_common_family([&first, &second].into_iter()),
            Some(IpFamily::V4)
        );

        first
            .endpoints
            .insert("[2001:db8::1]:40000".parse().unwrap());
        second
            .endpoints
            .insert("[2001:db8::2]:40001".parse().unwrap());
        assert_eq!(
            select_common_family([&first, &second].into_iter()),
            Some(IpFamily::V6)
        );
    }

    #[test]
    fn ipv6_rate_limit_key_uses_64_bit_prefix() {
        let first: IpAddr = "2001:db8:1234:5678::1".parse().unwrap();
        let same_prefix: IpAddr = "2001:db8:1234:5678:ffff::2".parse().unwrap();
        let other_prefix: IpAddr = "2001:db8:1234:5679::1".parse().unwrap();
        assert_eq!(ip_rate_key(first), ip_rate_key(same_prefix));
        assert_ne!(ip_rate_key(first), ip_rate_key(other_prefix));
    }

    #[tokio::test]
    async fn bounded_reader_rejects_oversized_lines() {
        let (mut writer, mut reader) = tokio::io::duplex(1024);
        writer
            .write_all(&vec![b'a'; MAX_SIGNAL_LINE + 1])
            .await
            .unwrap();
        writer.write_all(b"\n").await.unwrap();
        let error = read_bounded_line(&mut reader, &mut Vec::new())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn room_rate_limit_enforces_burst() {
        let state = Arc::new(Mutex::new(ServerState::default()));
        let ip: IpAddr = "203.0.113.10".parse().unwrap();
        for _ in 0..10 {
            assert!(rate_allow(&state, ip, RateClass::Room).await);
        }
        assert!(!rate_allow(&state, ip, RateClass::Room).await);
    }

    #[tokio::test]
    async fn registration_is_address_bound_and_single_use() {
        let state = Arc::new(Mutex::new(ServerState::default()));
        let (sender, _receiver) = mpsc::channel(1);
        let client_id = "00112233445566778899aabbccddeeff";
        let secret = [7_u8; 32];
        let client_nonce = [1_u8; 16];
        let server_nonce = [2_u8; 16];
        let source: SocketAddr = "203.0.113.2:40000".parse().unwrap();
        state.lock().await.clients.insert(
            client_id.to_owned(),
            ClientRecord {
                sender,
                registration_secret: secret,
                challenges: HashMap::from([(
                    IpFamily::V4,
                    RegistrationChallenge {
                        source,
                        client_nonce,
                        server_nonce,
                        expires_at: Instant::now() + Duration::from_secs(5),
                    },
                )]),
                supports_dual_stack: false,
            },
        );
        let payload = registration_payload(client_id, &client_nonce, &server_nonce, source);
        let mut mac = HmacSha256::new_from_slice(&secret).unwrap();
        mac.update(payload.as_bytes());
        let tag: [u8; 32] = mac.finalize().into_bytes().into();

        assert!(
            authenticate_registration(&state, client_id, source, client_nonce, server_nonce, tag,)
                .await
        );
        assert!(
            !authenticate_registration(&state, client_id, source, client_nonce, server_nonce, tag,)
                .await
        );
    }

    #[tokio::test]
    async fn registration_rejects_address_mismatch_and_expiry() {
        let state = Arc::new(Mutex::new(ServerState::default()));
        let (sender, _receiver) = mpsc::channel(1);
        let client_id = "ffeeddccbbaa99887766554433221100";
        let secret = [8_u8; 32];
        let client_nonce = [3_u8; 16];
        let server_nonce = [4_u8; 16];
        let source: SocketAddr = "203.0.113.5:41000".parse().unwrap();
        let wrong_source: SocketAddr = "203.0.113.5:41001".parse().unwrap();

        state.lock().await.clients.insert(
            client_id.to_owned(),
            ClientRecord {
                sender,
                registration_secret: secret,
                challenges: HashMap::from([(
                    IpFamily::V4,
                    RegistrationChallenge {
                        source,
                        client_nonce,
                        server_nonce,
                        expires_at: Instant::now() + Duration::from_secs(5),
                    },
                )]),
                supports_dual_stack: false,
            },
        );
        let payload = registration_payload(client_id, &client_nonce, &server_nonce, source);
        let mut mac = HmacSha256::new_from_slice(&secret).unwrap();
        mac.update(payload.as_bytes());
        let tag: [u8; 32] = mac.finalize().into_bytes().into();

        assert!(
            !authenticate_registration(
                &state,
                client_id,
                wrong_source,
                client_nonce,
                server_nonce,
                tag,
            )
            .await
        );

        state
            .lock()
            .await
            .clients
            .get_mut(client_id)
            .unwrap()
            .challenges
            .insert(
                IpFamily::V4,
                RegistrationChallenge {
                    source,
                    client_nonce,
                    server_nonce,
                    expires_at: Instant::now() - Duration::from_secs(1),
                },
            );
        assert!(
            !authenticate_registration(&state, client_id, source, client_nonce, server_nonce, tag,)
                .await
        );
    }

    #[tokio::test]
    async fn remaining_peer_keeps_room_and_becomes_creator() {
        let state = Arc::new(Mutex::new(ServerState::default()));
        let room_id = "00112233445566778899aabbccddeeff";
        let (creator_tx, _creator_rx) = mpsc::channel(4);
        let (joiner_tx, _joiner_rx) = mpsc::channel(4);
        let (replacement_tx, _replacement_rx) = mpsc::channel(4);

        create_room(&state, room_id, "creator", creator_tx)
            .await
            .expect("room creation should succeed");
        join_room(&state, room_id, "joiner", joiner_tx)
            .await
            .expect("first join should succeed");

        leave_room(&state, room_id, "creator").await;

        {
            let state = state.lock().await;
            let room = state.rooms.get(room_id).expect("room should remain");
            assert_eq!(room.creator, "joiner");
            assert_eq!(room.peers.len(), 1);
            assert!(room.peers.contains_key("joiner"));
        }

        join_room(&state, room_id, "replacement", replacement_tx)
            .await
            .expect("replacement peer should be able to join");
        assert_eq!(state.lock().await.rooms[room_id].peers.len(), 2);
    }

    #[tokio::test]
    async fn public_rooms_keep_full_rooms_at_bottom_and_reopened_rooms_move_to_top() {
        let state = Arc::new(Mutex::new(ServerState::default()));
        let old_room = "11111111111111111111111111111111";
        let new_room = "22222222222222222222222222222222";
        let (old_tx, mut old_rx) = mpsc::channel(16);
        let (new_tx, mut new_rx) = mpsc::channel(16);
        let (joiner_tx, mut joiner_rx) = mpsc::channel(16);

        create_public_room(&state, old_room, "오래된 방".to_owned(), "old", old_tx)
            .await
            .unwrap();
        create_public_room(&state, new_room, "새 방".to_owned(), "new", new_tx)
            .await
            .unwrap();
        join_public_room(&state, new_room, "joiner", joiner_tx)
            .await
            .unwrap();
        let _ = old_rx.recv().await;
        let _ = new_rx.recv().await;
        let _ = joiner_rx.recv().await;

        let first = collect_public_list(&state).await;
        assert!(first[1].starts_with(&format!("PUBLIC_ROOM {old_room} 1 ")));
        assert!(first[2].starts_with(&format!("PUBLIC_ROOM {new_room} 2 ")));

        leave_room(&state, new_room, "joiner").await;
        let reopened = collect_public_list(&state).await;
        assert!(reopened[1].starts_with(&format!("PUBLIC_ROOM {new_room} 1 ")));
        assert!(reopened[2].starts_with(&format!("PUBLIC_ROOM {old_room} 1 ")));
    }

    #[tokio::test]
    async fn private_and_public_join_commands_do_not_cross_room_types() {
        let state = Arc::new(Mutex::new(ServerState::default()));
        let private_id = "33333333333333333333333333333333";
        let public_id = "44444444444444444444444444444444";
        let (private_tx, _private_rx) = mpsc::channel(4);
        let (public_tx, _public_rx) = mpsc::channel(4);
        let (join_tx, _join_rx) = mpsc::channel(4);
        create_room(&state, private_id, "private", private_tx)
            .await
            .unwrap();
        create_public_room(&state, public_id, "공개방".to_owned(), "public", public_tx)
            .await
            .unwrap();

        assert_eq!(
            join_public_room(&state, private_id, "joiner", join_tx.clone()).await,
            Err("ROOM_NOT_FOUND")
        );
        assert_eq!(
            join_room(&state, public_id, "joiner", join_tx).await,
            Err("ROOM_NOT_FOUND")
        );
    }

    async fn collect_public_list(state: &SharedState) -> Vec<String> {
        let (sender, mut receiver) = mpsc::channel(128);
        send_public_rooms(state, &sender).await;
        drop(sender);
        let mut lines = Vec::new();
        while let Some(line) = receiver.recv().await {
            let done = line == "PUBLIC_LIST_END\n";
            lines.push(line);
            if done {
                break;
            }
        }
        lines
    }
}

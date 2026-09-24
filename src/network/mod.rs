pub mod client;

#[derive(Debug)]
pub enum NetworkCommand {
    CreateRoom,
    JoinRoom(String),
    LeaveRoom,
    SendMessage(String),
}

#[derive(Debug)]
pub enum NetworkEvent {
    ServerConnected,
    ServerDisconnected,

    RoomCreated(String),
    JoinedRoom(String),

    RoomFull,
    RoomNotFound,
    AlreadyInRoom,

    PeerConnected,
    PeerDisconnected,

    P2pFailed,

    MessageSent(String),
    MessageReceived(String),

    Error(String),
}

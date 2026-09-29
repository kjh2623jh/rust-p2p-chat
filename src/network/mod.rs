#[cfg(feature = "client")]
pub mod client;

use crate::protocol::InviteCode;

#[derive(Debug)]
pub enum NetworkCommand {
    CreateRoom,
    JoinRoom(InviteCode),
    LeaveRoom,
    SendMessage(String),
}

#[derive(Debug)]
pub enum NetworkEvent {
    ServerConnected,
    ServerDisconnected,

    RoomCreated { invite: String, fingerprint: String },
    JoinedRoom { fingerprint: String },

    RoomFull,
    RoomNotFound,
    AlreadyInRoom,

    PeerConnected,
    PeerDisconnected,

    P2pFailed,

    MessagePending { id: u64, text: String },
    MessageDelivered(u64),
    MessageFailed { id: u64, reason: String },
    MessageReceived(String),

    Error(NetworkError),
}

#[derive(Debug, Clone)]
pub enum NetworkError {
    InviteInvalid,
    TlsFailed(String),
    ProtocolMismatch,
    PeerAuthenticationFailed,
    RateLimited,
    MessageTooLong,
    Transport(String),
}

impl std::fmt::Display for NetworkError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InviteInvalid => write!(formatter, "초대 코드 형식이 올바르지 않습니다."),
            Self::TlsFailed(detail) => write!(formatter, "보안 연결에 실패했습니다: {detail}"),
            Self::ProtocolMismatch => write!(formatter, "서버와 앱의 프로토콜 버전이 다릅니다."),
            Self::PeerAuthenticationFailed => {
                write!(
                    formatter,
                    "상대방을 인증하지 못했습니다. 초대 코드를 다시 확인해 주세요."
                )
            }
            Self::RateLimited => write!(
                formatter,
                "요청이 너무 많습니다. 잠시 후 다시 시도해 주세요."
            ),
            Self::MessageTooLong => {
                write!(formatter, "메시지는 500자, 1024바이트 이하여야 합니다.")
            }
            Self::Transport(detail) => write!(formatter, "네트워크 오류: {detail}"),
        }
    }
}

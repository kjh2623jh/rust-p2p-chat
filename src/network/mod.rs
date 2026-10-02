#[cfg(feature = "client")]
pub mod client;

use crate::protocol::InviteCode;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicRoomSummary {
    pub room_id: String,
    pub title: String,
    pub occupants: u8,
}

impl PublicRoomSummary {
    pub fn is_full(&self) -> bool {
        self.occupants >= 2
    }
}

#[derive(Debug)]
pub enum NetworkCommand {
    CreateRoom {
        nickname: String,
    },
    CreatePublicRoom {
        title: String,
        nickname: String,
    },
    JoinRoom {
        invite: InviteCode,
        nickname: String,
    },
    JoinPublicRoom {
        room_id: String,
        nickname: String,
    },
    RefreshPublicRooms,
    RetryPeerConnection,
    LeaveRoom,
    SendMessage(String),
}

#[derive(Debug)]
pub enum NetworkEvent {
    ServerConnected,
    ServerDisconnected,

    RoomCreated {
        invite: String,
        fingerprint: String,
    },
    JoinedRoom {
        invite: String,
        fingerprint: String,
    },
    PublicRoomCreated {
        room_id: String,
        title: String,
    },
    PublicRoomJoined {
        room_id: String,
        title: String,
    },
    PublicRooms(Vec<PublicRoomSummary>),

    RoomFull,
    RoomNotFound,
    PublicRoomFull,
    PublicRoomNotFound,
    AlreadyInRoom,

    PeerConnected {
        nickname: String,
        safety_number: Option<String>,
    },
    PeerConnecting,
    PeerDisconnected,
    PeerConnectionFailed(PeerConnectionFailure),
    RoomExpired,

    MessagePending {
        id: u64,
        text: String,
    },
    MessageDelivered(u64),
    MessageFailed {
        id: u64,
        reason: String,
    },
    MessageReceived(String),

    Error(NetworkError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerConnectionFailure {
    UdpRegistration,
    HandshakeTimedOut,
    ConnectionLost,
    SecurityError,
    NoCompatibleNetwork,
}

#[derive(Debug, Clone)]
pub enum NetworkError {
    InviteInvalid,
    TlsFailed(String),
    ProtocolMismatch,
    PeerAuthenticationFailed,
    RateLimited,
    PublicRoomLimit,
    NicknameInvalid,
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
            Self::PublicRoomLimit => write!(
                formatter,
                "공개방이 너무 많아 지금은 새 공개방을 만들 수 없습니다. 잠시 후 다시 시도해 주세요."
            ),
            Self::NicknameInvalid => {
                write!(formatter, "닉네임 형식이 올바르지 않습니다.")
            }
            Self::MessageTooLong => {
                write!(formatter, "메시지는 500자, 1024바이트 이하여야 합니다.")
            }
            Self::Transport(detail) => write!(formatter, "네트워크 오류: {detail}"),
        }
    }
}

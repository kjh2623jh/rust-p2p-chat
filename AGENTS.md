# P2P Chat

## 프로젝트 개요

이 저장소는 서버가 채팅 내용을 중계하지 않는 1:1 UDP P2P 채팅 데스크톱 애플리케이션이다.

공개 signaling 서버는 방 관리와 peer 연결 정보 교환만 담당한다. 실제 채팅 메시지는 signaling 서버를 거치지 않고 두 클라이언트가 UDP로 직접 주고받는다.

네트워크는 현재 IPv4를 사용하며, 데스크톱 GUI는 `eframe`과 `egui`로 구성되어 있다.

## 아키텍처

- TCP signaling
  - 클라이언트 식별
  - 방 생성 및 입장
  - peer 연결 정보 전달
  - 퇴장 및 연결 종료 처리
- UDP signaling
  - signaling 서버가 클라이언트의 공개 UDP endpoint를 확인
- UDP P2P
  - UDP hole punching
  - 클라이언트 간 직접 채팅 메시지 송수신
- GUI
  - 채널을 통해 네트워크 작업에 명령을 전달하고 이벤트를 수신
- 비동기 실행
  - Tokio runtime을 별도 네트워크 스레드에서 실행

## 저장소 구조

```text
src/
├─ bin/
│  ├─ server.rs
│  └─ client.rs
├─ network/
│  ├─ mod.rs
│  └─ client.rs
├─ app.rs
└─ lib.rs
```

### `src/bin/server.rs`

공개 signaling 서버 구현이다.

- TCP signaling 연결 처리
- 방 생성, 입장, 퇴장 및 정리
- UDP endpoint 등록
- 두 peer의 공개 UDP endpoint 교환

### `src/bin/client.rs`

데스크톱 클라이언트의 진입점이다.

- Tokio networking runtime 생성
- 네트워크 스레드 실행
- GUI와 네트워크 사이의 채널 생성
- eframe 애플리케이션 실행

### `src/app.rs`

`egui` 기반 GUI와 애플리케이션 상태를 관리한다.

- 방 생성 및 입장 UI
- 연결 상태 표시
- 채팅 메시지 입력 및 출력
- `NetworkCommand` 전송
- `NetworkEvent` 처리
- 한국어 폰트 등록

### `src/network/mod.rs`

GUI와 네트워크 계층이 사용하는 명령과 이벤트 타입을 정의한다.

### `src/network/client.rs`

클라이언트 네트워크 로직을 담당한다.

- TCP signaling 연결
- UDP endpoint 등록
- UDP hole punching
- P2P 상태 관리
- 직접 채팅 메시지 송수신

### `src/lib.rs`

애플리케이션과 네트워크 모듈을 외부에 공개한다.

## 주요 기술

- Rust 2024 edition
- Tokio
- eframe / egui 0.36
- rand 0.9
- TCP signaling
- UDP hole punching
- IPv4 networking

## 네트워크 프로토콜

### 클라이언트 → 서버 TCP

```text
CREATE
JOIN <room_code>
LEAVE
P2P_FAILED
```

### 서버 → 클라이언트 TCP

```text
CLIENT_ID <id>
CREATED <room_code>
JOINED <room_code>
ROOM_NOT_FOUND
ROOM_FULL
ALREADY_IN_ROOM
PEER <ip:port>
PEER_LEFT <client_id>
LEFT
P2P_DISCONNECTED
```

### 클라이언트 → 서버 UDP

```text
REGISTER <client_id>
```

### 클라이언트 ↔ 클라이언트 UDP

```text
PUNCH
PUNCH_ACK
```

위 제어 패킷이 아닌 UDP payload는 채팅 메시지로 처리한다.

## 방 동작

- 방은 최대 두 명이 참여하는 1:1 구조다.
- 서버는 방 생성 시 6자리 방 코드를 만든다.
- 방 코드에서는 `O`, `0`, `I`, `1`, `L`처럼 혼동하기 쉬운 문자를 사용하지 않는다.
- `JOIN`은 이미 존재하는 방에만 입장한다.
- 존재하지 않는 방은 `ROOM_NOT_FOUND`를 반환한다.
- 두 명이 참여 중인 방은 `ROOM_FULL`을 반환한다.
- 마지막 사용자가 나가면 빈 방을 서버 상태에서 제거한다.

## P2P 연결 흐름

```text
클라이언트 A가 방 생성
→ 서버가 CREATED 응답
→ A가 UDP REGISTER 전송

클라이언트 B가 방 입장
→ 서버가 JOINED 응답
→ B가 UDP REGISTER 전송

서버가 두 클라이언트의 공개 UDP endpoint 확인
→ 양쪽에 PEER 전달
→ 양쪽이 PUNCH 반복 전송
→ PUNCH 수신 시 PUNCH_ACK 응답
→ PUNCH_ACK 수신 시 P2P 연결 완료
→ UDP로 채팅 메시지 직접 송수신
```

UDP 등록, hole punching, 채팅 메시지 송수신에는 동일한 UDP socket을 사용한다.

## GUI와 네트워크 통신

GUI와 네트워크 계층은 bounded channel로 분리되어 있다.

### GUI → 네트워크

```rust
pub enum NetworkCommand {
    CreateRoom,
    JoinRoom(String),
    LeaveRoom,
    SendMessage(String),
}
```

### 네트워크 → GUI

```rust
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
```

## 실행 환경

Windows 개발 환경에서는 `wgpu`의 DX12 backend를 사용한다.

```powershell
$env:WGPU_BACKEND="dx12"
cargo run --bin client
```

Signaling 서버는 다음 명령으로 실행한다.

```bash
cargo run --bin server
```

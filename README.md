# P2P Chat

서버에 대화 내용을 남기지 않고, 두 사용자가 직접 연결해 대화하는 1:1 데스크톱 채팅 앱입니다.

P2P Chat은 계정이나 연락처 등록 없이 공개방 목록 또는 안전한 비공개 초대 코드로 상대방을 만납니다. signaling 서버는 방 생성과 상대방 연결만 도우며, 실제 채팅 메시지는 서버를 거치지 않고 두 기기 사이에서 암호화되어 전송됩니다.

[최신 버전 다운로드](https://github.com/kjh2623jh/rust-p2p-chat/releases/latest) · [보안 정책](SECURITY.md)

> **AI 활용 안내**
>
> 이 프로젝트는 기획, UI/UX 설계, 코드 작성, 리팩터링 및 문서화 과정에서 생성형 AI의 도움을 받아 제작되었습니다. 기능에 대한 최종 결정과 테스트, 배포는 프로젝트 관리자가 담당합니다.

## 주요 기능

- **직접 연결:** 채팅 메시지를 중앙 서버가 중계하거나 저장하지 않습니다.
- **종단간 암호화:** Noise Protocol 기반 암호화 세션으로 메시지를 보호합니다.
- **공개방 목록:** 공개방은 코드 없이 입장하며, 만석인 방도 상태를 확인할 수 있도록 목록 아래에 표시됩니다.
- **비공개 초대:** 목록에 노출되지 않는 방은 인증된 보안 초대 코드로만 입장합니다.
- **암호화된 닉네임 교환:** 닉네임은 signaling 서버가 아니라 연결된 상대에게만 암호화해 전달합니다.
- **1:1 대화:** 방마다 최대 두 명만 참여할 수 있습니다.
- **전송 신뢰성:** 암호화된 ACK, 중복 제거, 제한된 재전송을 지원합니다.
- **편리한 데스크톱 UI:** Enter로 방 입장과 메시지 전송이 가능하며, 연결 상태와 오류를 화면에서 확인할 수 있습니다.
- **연결 복구 흐름:** 상대방이 나가거나 P2P 연결이 끊겨도 방을 유지하며, 새 상대방을 기다리거나 연결을 다시 시도할 수 있습니다.
- **IPv4/IPv6 dual-stack:** 두 환경에서 모두 사용할 수 있으면 IPv6를 우선하고, 그렇지 않으면 IPv4로 자동 전환합니다.
- **Windows 렌더러 fallback:** DX12, Glow, Vulkan 순서로 별도 프로세스를 실행해 그래픽 드라이버 충돌에도 다음 렌더러를 시도합니다.

## 작동 방식

```text
┌────────────┐      TLS 1.3      ┌──────────────────┐      TLS 1.3      ┌────────────┐
│  Client A  │ ◀───────────────▶│ Signaling Server │◀───────────────▶│  Client B  │
└─────┬──────┘                   └──────────────────┘                   └──────┬─────┘
      │                                                                        │
      └──────────── Noise로 암호화된 UDP P2P 메시지 직접 전송 ─────────────────────┘
```

1. Client A가 공개방을 만들거나 비공개방과 안전한 초대 코드를 만듭니다.
2. Client B가 공개 목록 또는 전달받은 비공개 초대 코드로 방에 입장합니다.
3. 서버가 두 클라이언트에 공통으로 존재하는 IPv6 또는 IPv4 UDP endpoint를 선택해 전달합니다.
4. 두 클라이언트가 UDP hole punching을 수행하고 암호화 세션을 만듭니다.
5. 이후 채팅 메시지는 signaling 서버를 거치지 않고 두 클라이언트 사이에서 직접 오갑니다.

## 다운로드 및 설치

패키징된 앱은 [GitHub Releases](https://github.com/kjh2623jh/rust-p2p-chat/releases)에서 받을 수 있습니다.

공식 배포본은 기본적으로 `p2psignal.mcv.kr` 시그널링 서버에 연결하며, 사용하려면 인터넷 연결이 필요합니다.

### Windows

1. `p2p-chat-vX.Y.Z-windows-x86_64.exe`를 다운로드합니다.
2. 별도 설치 없이 실행 파일을 실행합니다.

현재 Windows 실행 파일에는 Authenticode 코드 서명이 없어 SmartScreen 경고가 나타날 수 있습니다. 출처가 이 저장소의 공식 Release인지 확인하고, 아래의 체크섬 검증 방법을 이용하세요.

### macOS

1. `p2p-chat-vX.Y.Z-macos-universal.dmg`를 다운로드합니다.
2. DMG를 열고 **P2P Chat**을 **Applications** 폴더로 옮깁니다.

macOS 11 이상에서 사용할 수 있으며, 하나의 Universal 앱이 Apple Silicon과 Intel Mac을 모두 지원합니다. 배포 파일은 Developer ID로 서명하고 Apple 공증 및 stapling 검증을 거쳐 생성됩니다.

### 다운로드 파일 검증

각 Release의 `SHA256SUMS`에는 배포 파일의 SHA-256 해시가 들어 있습니다.

Windows PowerShell:

```powershell
Get-FileHash .\p2p-chat-vX.Y.Z-windows-x86_64.exe -Algorithm SHA256
```

macOS:

```bash
shasum -a 256 p2p-chat-vX.Y.Z-macos-universal.dmg
```

출력된 해시가 같은 Release의 `SHA256SUMS`와 일치하는지 확인하세요. GitHub artifact attestation도 함께 제공됩니다.

## 사용 방법

로비의 **내 닉네임**에서 이번 실행에 사용할 이름을 설정합니다. 처음 실행하면 임의의 `익명-0000` 형식이 제안되며, 닉네임은 최대 20자입니다. 닉네임은 계정이나 신원 인증 수단이 아니며 앱을 종료하면 저장되지 않습니다.

### 공개방 만들기와 입장

1. **공개방**을 선택하고 목록에 표시할 이름을 입력합니다.
2. 공개방을 만들면 다른 사용자가 목록에서 코드 없이 입장할 수 있습니다.
3. `1 / 2명`인 방은 입장할 수 있고, `2 / 2명`인 방은 목록 아래에 비활성 상태로 남습니다.
4. 만석인 방에서 한 명이 나가면 방이 다시 활성화되어 목록 위쪽으로 이동합니다.
5. 상대를 별도 채널로 확인해야 한다면 **보안 정보**를 열어 양쪽 화면의 안전번호가 같은지 비교합니다.

### 비공개방 만들기

1. 앱을 실행하고 **비공개방**을 선택해 방을 만듭니다.
2. 생성된 초대 코드를 복사합니다.
3. 상대방에게 신뢰할 수 있는 별도 채널로 초대 코드를 전달합니다.
4. 상대방이 입장하면 P2P 연결이 완료될 때까지 잠시 기다립니다.

### 비공개방 입장하기

1. 전달받은 초대 코드를 입력란에 붙여 넣습니다.
2. **입장** 버튼을 누르거나 Enter를 입력합니다.
3. P2P 연결 상태가 연결됨으로 바뀌면 메시지를 보낼 수 있습니다.

메시지는 입력 후 Enter로 전송할 수 있습니다. 한 메시지는 최대 500자, 1,024바이트까지 지원합니다.

상대방이 나가면 대화 화면과 방은 유지되며, 같은 초대 코드로 다시 참여할 수 있습니다. 다만 혼자 남은 상태가 10분 이상 이어진 방은 서버에서 정리될 수 있습니다.

## 개인정보와 보안

P2P Chat이 보호하는 범위와 P2P 방식 자체의 한계를 함께 확인해 주세요.

- signaling 연결은 TLS 1.3으로 보호됩니다.
- 비공개방 채팅은 초대 비밀을 사용하는 `Noise_NNpsk0_25519_ChaChaPoly_SHA256` 세션으로 인증·암호화됩니다.
- 공개방 채팅은 `Noise_NN_25519_ChaChaPoly_SHA256` 세션으로 암호화되지만 초대 비밀 기반 인증은 없습니다. 표시되는 안전번호를 상대방과 비교해야 signaling 서버를 포함한 능동적 중간자 공격을 탐지할 수 있습니다.
- 공개방 목록에는 방 ID, 제목, 현재 인원만 표시되며 채팅 본문은 포함되지 않습니다.
- 초대 비밀, 닉네임과 채팅 본문은 signaling 서버에 전송하거나 저장하지 않습니다. 닉네임은 Noise 세션이 완성된 뒤 암호화된 P2P 패킷으로 교환됩니다.
- 서버 로그에는 전체 방 ID, 초대 코드, 메시지 본문, 전체 endpoint를 기록하지 않습니다.
- 초대 코드를 가진 사람은 방이 만료되기 전에 참여할 수 있습니다. 초대 코드를 공개 채널에 올리지 마세요.
- 직접 P2P 연결 특성상 **상대방과 signaling 서버는 사용자의 공개 IPv4 또는 IPv6 주소를 알 수 있습니다.** 현재 relay, VPN, Tor 같은 IP 익명화 기능은 제공하지 않습니다.
- 대칭 NAT나 엄격한 회사·학교 네트워크에서는 UDP hole punching이 실패할 수 있습니다. 현재 relay fallback은 없습니다.

자세한 설계, 알려진 제한, 취약점 제보 방법은 [SECURITY.md](SECURITY.md)를 참고하세요. 초대 코드, IP 주소, 인증서 개인 키나 개인정보가 포함된 보안 문제는 공개 Issue가 아닌 GitHub Private Vulnerability Reporting으로 알려 주세요.

## 소스에서 빌드하기

### 준비 사항

- 최신 Rust stable toolchain
- Git
- 운영체제별 eframe 빌드 의존성

저장소를 복제한 뒤 다음 명령을 실행합니다.

```bash
git clone https://github.com/kjh2623jh/rust-p2p-chat.git
cd rust-p2p-chat
cargo build --locked --release --features client --bin client
```

개발 모드로 클라이언트를 실행하려면:

```bash
cargo run --features client --bin client
```

Windows에서는 인자 없이 실행하면 launcher가 GUI를 자식 프로세스로 시작합니다. 기본 순서는 다음과 같습니다.

```text
DX12 → 실패 시 Glow → 실패 시 Vulkan
```

특정 렌더러를 직접 시험할 수도 있습니다.

```powershell
cargo run --features client --bin client -- --renderer dx12
cargo run --features client --bin client -- --renderer glow
cargo run --features client --bin client -- --renderer vulkan
```

Windows용 다중 해상도 ICO를 마스터 PNG에서 다시 만들려면 Windows PowerShell에서 다음을 실행합니다.

```powershell
.\scripts\generate-windows-icon.ps1
```

macOS용 ICNS는 release workflow가 같은 `assets/app-icon.png`에서 자동 생성합니다.

## signaling 서버 직접 운영하기

서버 바이너리는 GUI 의존성 없이 별도로 빌드됩니다.

공개방 목록, 암호화된 닉네임 교환, 방을 유지하는 P2P 재연결은 signaling protocol v6를 사용합니다. 새 클라이언트를 배포하기 전에 같은 커밋의 서버를 먼저 배포해야 하며, 이전 protocol 클라이언트와 서버는 v6와 연결되지 않습니다.

```bash
cargo build --locked --release --no-default-features --bin server
```

서버는 PEM 형식의 인증서 체인과 개인 키가 필요하며 TLS 1.3만 허용합니다.

```bash
export P2P_TLS_CERT_PATH=/etc/letsencrypt/live/chat.example.com/fullchain.pem
export P2P_TLS_KEY_PATH=/etc/letsencrypt/live/chat.example.com/privkey.pem
export P2P_TCP_BIND=0.0.0.0:9000
export P2P_UDP_BIND=0.0.0.0:9001
export P2P_TCP_BIND_V6='[::]:9000'
export P2P_UDP_BIND_V6='[::]:9001'
RUST_LOG=info ./target/release/server
```

`P2P_TCP_BIND_V6`와 `P2P_UDP_BIND_V6`는 선택 사항입니다. 서버에 공용 IPv6 주소와 IPv6 방화벽·라우팅이 준비된 경우에만 설정하세요. IPv4와 IPv6 listener는 분리되어 있으며, IPv6 listener는 IPv4-mapped 연결을 받지 않습니다.

방화벽과 클라우드 네트워크 보안 규칙에서 다음 포트를 허용해야 합니다.

| 용도                      | 프로토콜 | 기본 포트 |
| ------------------------- | -------- | --------: |
| TLS signaling             | TCP      |      9000 |
| UDP 등록 및 P2P 연결 보조 | UDP      |      9001 |

IPv6를 제공하려면 서버 도메인에 A 레코드와 함께 AAAA 레코드를 추가해야 합니다. AAAA 레코드는 IPv6 listener와 외부 연결을 먼저 검증한 후 공개하세요.

클라이언트가 자체 서버를 사용하도록 하려면 서버 인증서의 DNS 이름과 접속 주소를 지정합니다.

```powershell
$env:P2P_SERVER_NAME = "chat.example.com"
$env:P2P_TCP_SERVER = "chat.example.com:9000"
$env:P2P_UDP_SERVER = "chat.example.com:9001"
cargo run --features client --bin client
```

인증서 검증을 끄는 fallback은 제공하지 않습니다. 인증서 갱신 후에는 서버 프로세스가 새 인증서와 키를 읽도록 안전하게 재시작하세요.

## 프로젝트 구조

```text
assets/
├─ app-icon.png         # 공통 1024px 앱 아이콘
└─ app-icon.ico         # Windows 다중 해상도 실행 파일 아이콘
scripts/
└─ generate-windows-icon.ps1
src/
├─ bin/
│  ├─ client.rs          # 데스크톱 클라이언트 진입점
│  ├─ client/renderer.rs # Windows 렌더러 launcher와 fallback
│  └─ server.rs          # signaling 서버
├─ network/
│  ├─ mod.rs             # GUI와 네트워크 사이의 명령/이벤트
│  └─ client.rs          # TLS signaling, UDP hole punching, P2P 송수신
├─ app.rs                # egui 기반 UI와 애플리케이션 상태
├─ protocol.rs           # 초대 코드와 프로토콜 공통 타입
└─ lib.rs
```

클라이언트 GUI는 `client` Cargo feature로 분리되어 있어 서버 빌드에는 `eframe`, `wgpu`, Noise 클라이언트 코드가 포함되지 않습니다.

## 개발 확인

변경 사항을 제출하기 전 다음 검사를 권장합니다.

```bash
cargo fmt --check
cargo test --locked --all-targets --all-features
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo build --locked --release --no-default-features --bin server
```

## 릴리스

`Cargo.toml`의 버전과 같은 `vX.Y.Z` 태그를 push하면 GitHub Actions가 테스트, Clippy, 의존성 감사와 빌드를 수행합니다. 성공한 결과는 바로 공개되지 않고 **Draft Release**로 생성됩니다.

Draft에는 다음 파일이 포함됩니다.

- Windows x86_64 실행 파일
- Apple Silicon과 Intel을 함께 지원하는 macOS Universal DMG
- 배포 파일 검증용 `SHA256SUMS`

Windows 실행 파일과 macOS DMG에는 각각 GitHub artifact attestation이 발급됩니다.

실제 네트워크에서 새 클라이언트와 signaling 서버의 호환성을 확인한 뒤 Draft를 공개하세요.

<details>
<summary>macOS 릴리스 관리자 설정</summary>

GitHub Actions의 Repository settings에 다음 값을 등록해야 합니다.

- Secrets: `APPLE_CERTIFICATE_P12_BASE64`, `APPLE_CERTIFICATE_PASSWORD`, `APPLE_API_KEY_P8_BASE64`, `APPLE_API_KEY_ID`, `APPLE_API_ISSUER_ID`
- Variables: `APPLE_SIGNING_IDENTITY`, `APPLE_TEAM_ID`, `APPLE_BUNDLE_ID`

인증서는 Developer ID Application `.p12`, API 키는 App Store Connect Team API key를 사용합니다. 권장 bundle identifier는 `com.github.kjh2623jh.p2p-chat`입니다.

</details>

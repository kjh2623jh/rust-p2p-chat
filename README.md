# P2P Chat

서버가 메시지를 중계하거나 저장하지 않는 1:1 데스크톱 채팅입니다. signaling 연결은 TLS 1.3을 사용하고, peer 사이의 UDP 메시지는 보안 초대 코드를 PSK로 사용하는 Noise 세션으로 종단간 암호화됩니다.

> 직접 P2P 연결 특성상 대화 상대와 signaling 서버는 사용자의 공개 IP 주소를 확인할 수 있습니다. IP 주소까지 숨겨야 한다면 이 앱 대신 신뢰할 수 있는 relay/Tor 기반 서비스를 사용해야 합니다.

## 빌드

서버 빌드에는 GUI, `eframe`, `wgpu`, Noise client 코드가 포함되지 않습니다.

```bash
cargo build --locked --release --no-default-features --bin server
cargo build --locked --release --features client --bin client
```

Windows release client는 GUI subsystem으로 빌드되어 별도 명령 프롬프트 창을 열지 않습니다.

## signaling 서버 실행

서버는 PEM 형식의 인증서 체인과 개인 키를 필수로 요구하며 TLS 1.3만 허용합니다.

```bash
export P2P_TLS_CERT_PATH=/etc/letsencrypt/live/p2psignal.mcv.kr/fullchain.pem
export P2P_TLS_KEY_PATH=/etc/letsencrypt/live/p2psignal.mcv.kr/privkey.pem
RUST_LOG=info ./server
```

기본 TCP/UDP 포트는 각각 `9000`, `9001`입니다. `P2P_TCP_BIND`, `P2P_UDP_BIND`로 bind 주소를 변경할 수 있습니다. 클라이언트는 기본적으로 `p2psignal.mcv.kr` 인증서를 검증하며, 개발 환경에서는 `P2P_SERVER_NAME`, `P2P_TCP_SERVER`, `P2P_UDP_SERVER`로 목적지만 바꿀 수 있습니다. 인증서 검증을 끄는 fallback은 없습니다.

Let's Encrypt를 사용한다면 인증서 갱신 뒤 서버를 안전하게 재시작하는 deploy hook을 운영 환경에 추가하세요. 개인 키 파일은 서버 프로세스 계정만 읽을 수 있게 제한해야 합니다.

## 릴리스

`v*` 태그를 push하면 GitHub Actions가 테스트, Clippy, 의존성 감사를 수행한 뒤 Windows와 두 macOS 아키텍처를 빌드합니다. 성공한 결과는 즉시 공개되지 않고 **Draft Release**로 생성됩니다. v2 signaling 서버를 먼저 배포하고 서로 다른 외부 네트워크의 두 기기로 연결을 확인한 뒤 draft를 수동 공개하세요.

macOS 빌드에는 다음 GitHub Actions 설정이 필요합니다.

- Secrets: `APPLE_CERTIFICATE_P12_BASE64`, `APPLE_CERTIFICATE_PASSWORD`, `APPLE_API_KEY_P8_BASE64`, `APPLE_API_KEY_ID`, `APPLE_API_ISSUER_ID`
- Variables: `APPLE_SIGNING_IDENTITY`, `APPLE_TEAM_ID`, `APPLE_BUNDLE_ID` (`com.github.kjh2623jh.p2p-chat` 권장)

인증서는 Developer ID Application `.p12`, API 키는 App Store Connect Team API key여야 합니다. 앱은 hardened runtime으로 서명되고 Apple 공증 및 stapling 검증을 통과해야 업로드됩니다.

Windows 실행 파일은 현재 코드 서명되지 않았으므로 SmartScreen 경고가 나타날 수 있습니다. 릴리스의 `SHA256SUMS` 및 GitHub artifact attestation으로 내려받은 파일을 검증할 수 있습니다.

### v0.2 배포 체크리스트

1. 인증서 경로와 자동 갱신 deploy hook을 준비하고, 로컬/스테이징에서 TLS v2 server-client 연결을 확인합니다.
2. `v0.2.0` 태그를 push해 Draft Release 산출물을 생성합니다.
3. 짧은 점검 시간을 공지한 뒤 기존 signaling 서버를 v2 서버로 교체합니다. v1 평문 fallback은 제공하지 않습니다.
4. 서로 다른 외부 네트워크의 두 기기에서 방 생성, 입장, 암호화 연결, 메시지 ACK, 연결 종료 후 로비 복귀를 확인합니다.
5. 성공하면 draft를 공개합니다. 실패하면 서버를 이전 버전으로 되돌리고 draft는 공개하지 않은 채 원인을 수정합니다.

## 테스트

```bash
cargo test --locked --all-targets --all-features
cargo clippy --locked --all-targets --all-features -- -D warnings
```

보안 설계와 제한 사항은 [SECURITY.md](SECURITY.md)를 참고하세요.

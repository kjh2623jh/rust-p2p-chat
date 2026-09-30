# Security

## 보호 범위

- signaling: TLS 1.3, 운영체제/WebPKI 신뢰 저장소 기반 서버 인증
- UDP endpoint 등록: TLS에서 전달한 256-bit 비밀과 HMAC-SHA256 challenge, 5초 만료, 1회 사용, 관측 source address 결합
- 비공개방 P2P 메시지: `Noise_NNpsk0_25519_ChaChaPoly_SHA256`, 256-bit 초대 비밀, 방 ID와 pairing nonce를 prologue에 결합
- 공개방 P2P 메시지: `Noise_NN_25519_ChaChaPoly_SHA256`, 방 ID와 pairing nonce를 prologue에 결합, handshake hash에서 파생한 안전번호 표시
- 닉네임: Noise 세션 안의 암호화된 준비 패킷으로 peer끼리만 교환, signaling 서버에는 미전송
- UDP replay 방지: 명시적 64-bit sequence와 128-packet sliding window
- 전송 신뢰성: 암호화된 message ACK, 중복 제거, 제한된 재전송 및 pending queue
- 자원 제한: signaling line/datagram/message 크기, 연결/IP/request rate, 방 TTL, 채팅 기록 상한

초대 비밀과 채팅 본문은 signaling 서버에 전송하거나 저장하지 않습니다. 공개방 목록을 위해 서버에는 공개방 ID, 제목, 현재 인원이 유지됩니다. 로그에는 전체 방 ID, 초대 코드, 메시지, 전체 endpoint를 기록하지 않습니다.

## 알려진 제한

- 직접 UDP P2P 연결이므로 상대방과 signaling 서버가 공개 IPv4 또는 IPv6 주소를 알 수 있습니다. 현재 relay/IP 익명화 기능은 없습니다.
- 방 참가자는 초대 코드 보유 여부로 인증됩니다. 코드를 전달받은 누구나 만료 전 방에 참가할 수 있으므로 안전한 채널로 공유해야 합니다.
- 공개방에는 사전 공유 비밀이 없으므로 암호화만으로 상대 신원을 인증하지 않습니다. signaling 서버가 능동적으로 개입하는 중간자 공격까지 탐지하려면 양쪽 화면의 안전번호를 신뢰할 수 있는 별도 채널로 비교해야 합니다.
- 공개방 제목은 모든 사용자에게 공개됩니다. 개인정보, 초대 코드 또는 민감한 내용을 방 제목에 넣지 마세요.
- 닉네임은 사용자가 임의로 정하며 고유성이나 실제 신원을 보장하지 않습니다. 공개방에서는 닉네임이 아니라 안전번호 비교로 현재 암호화 연결을 확인하세요.
- 서버는 두 peer의 연결 정보만 교환하며 메시지 전달을 보장하지 않습니다.
- `snow` 구현은 프로젝트 차원의 독립 공식 보안 감사를 받지 않았다고 upstream이 고지합니다. 고위험·규제 환경에서는 별도 외부 감사를 거치기 전 사용하지 마세요.
- Windows 바이너리는 현재 Authenticode 서명이 없습니다.

## 취약점 제보

공개 issue에 초대 코드, IP 주소, 인증서 개인 키, 재현용 개인정보를 올리지 마세요. 저장소 관리자가 제공하는 비공개 보안 제보 채널(GitHub Private Vulnerability Reporting)을 사용해 주세요.

제보에는 영향받는 버전, 재현 조건, 예상 영향, 가능한 완화책을 포함해 주세요.

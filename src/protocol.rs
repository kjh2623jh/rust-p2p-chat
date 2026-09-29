use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use rand::RngCore;
use sha2::{Digest, Sha256};
use std::{fmt, str::FromStr};
use zeroize::Zeroize;

pub const PROTOCOL_VERSION: u8 = 2;
pub const MAX_SIGNAL_LINE: usize = 512;
pub const MAX_DATAGRAM: usize = 1200;
pub const MAX_MESSAGE_CHARS: usize = 500;
pub const MAX_MESSAGE_BYTES: usize = 1024;
pub const INVITE_PREFIX: &str = "P2P2-";
const ROOM_CONTEXT: &[u8] = b"p2p-chat/room/v2";
const FINGERPRINT_ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

pub struct InviteCode([u8; 32]);

impl InviteCode {
    pub fn generate() -> Self {
        let mut secret = [0_u8; 32];
        rand::rng().fill_bytes(&mut secret);
        Self(secret)
    }

    pub fn secret(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn room_id(&self) -> [u8; 16] {
        let mut digest = Sha256::new();
        digest.update(ROOM_CONTEXT);
        digest.update(self.0);
        digest.finalize()[..16]
            .try_into()
            .expect("fixed digest length")
    }

    pub fn room_id_hex(&self) -> String {
        encode_hex(&self.room_id())
    }

    pub fn fingerprint(&self) -> String {
        let digest = Sha256::digest(self.0);
        (0..6)
            .map(|index| FINGERPRINT_ALPHABET[(digest[index] & 31) as usize] as char)
            .collect()
    }

    pub fn expose(&self) -> String {
        format!("{INVITE_PREFIX}{}", URL_SAFE_NO_PAD.encode(self.0))
    }
}

impl Clone for InviteCode {
    fn clone(&self) -> Self {
        Self(self.0)
    }
}

impl Drop for InviteCode {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

impl fmt::Debug for InviteCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InviteCode")
            .field("fingerprint", &self.fingerprint())
            .field("secret", &"[REDACTED]")
            .finish()
    }
}

impl FromStr for InviteCode {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value.trim();
        let encoded = value.strip_prefix(INVITE_PREFIX).ok_or(())?;
        let decoded = URL_SAFE_NO_PAD.decode(encoded).map_err(|_| ())?;
        let secret: [u8; 32] = decoded.try_into().map_err(|_| ())?;
        if URL_SAFE_NO_PAD.encode(secret) != encoded {
            return Err(());
        }
        Ok(Self(secret))
    }
}

pub fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        result.push(HEX[(byte >> 4) as usize] as char);
        result.push(HEX[(byte & 0x0f) as usize] as char);
    }
    result
}

pub fn decode_hex<const N: usize>(value: &str) -> Option<[u8; N]> {
    if value.len() != N * 2 || !value.is_ascii() {
        return None;
    }
    let mut result = [0_u8; N];
    let bytes = value.as_bytes();
    for index in 0..N {
        result[index] = (hex_nibble(bytes[index * 2])? << 4) | hex_nibble(bytes[index * 2 + 1])?;
    }
    Some(result)
}

fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invite_round_trip_and_redaction() {
        let invite = InviteCode::generate();
        let encoded = invite.expose();
        let parsed: InviteCode = encoded.parse().unwrap();
        assert_eq!(invite.secret(), parsed.secret());
        assert_eq!(invite.room_id(), parsed.room_id());
        assert_eq!(invite.fingerprint().len(), 6);
        assert!(!format!("{invite:?}").contains(&encoded[INVITE_PREFIX.len()..]));
    }

    #[test]
    fn rejects_non_canonical_invites() {
        assert!("ABC123".parse::<InviteCode>().is_err());
        assert!("P2P2-short".parse::<InviteCode>().is_err());
        assert!(
            format!("{}{}=", INVITE_PREFIX, URL_SAFE_NO_PAD.encode([7_u8; 32]))
                .parse::<InviteCode>()
                .is_err()
        );
    }

    #[test]
    fn fixed_length_hex_round_trip() {
        let source = [0, 1, 15, 16, 254, 255];
        assert_eq!(decode_hex::<6>(&encode_hex(&source)), Some(source));
        assert_eq!(decode_hex::<6>("zzzzzzzzzzzz"), None);
    }
}

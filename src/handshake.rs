use futures_util::SinkExt;
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec, LinesCodecError};

#[derive(Clone, Serialize, Deserialize, Debug)]
enum Codec {
    Lines,
    Unsafe,
}

#[derive(Clone, Deserialize, Debug)]
enum Mode {
    Produce,
    Consume,
}

impl Default for Codec {
    fn default() -> Self {
        Self::Lines
    }
}

#[derive(Deserialize, Debug)]
pub struct Handshake {
    topic: String,
    mode: Mode,
    #[serde(default)]
    codec: Codec,
}

#[derive(Serialize, Debug)]
struct Response {
    status: Result<(), String>,
}

#[derive(thiserror::Error, Debug)]
pub enum HandshakeError {
    #[error(transparent)]
    LinesCodecError(#[from] LinesCodecError),
    #[error("received empty message")]
    EmptyMessage,
    #[error(transparent)]
    JsonError(#[from] serde_json::Error),
}

pub async fn handle_new_client(sock: TcpStream) {
    let handshake_codec = LinesCodec::new_with_max_length(u16::MAX as usize);
    let mut handshake_framed = Framed::new(sock, handshake_codec);

    let hs = match try_handshake(&mut handshake_framed).await {
        Ok(hs) => hs,
        Err(e) => {
            let res = Response {
                status: Err(e.to_string()),
            };
            handshake_framed.send(serde_json::to_string(&res).expect("to serialize"));
            return;
        }
    };

    match hs.mode.clone() {
        Mode::Produce => {}
        Mode::Consume => {}
    }
}

async fn handle_produce() {}
async fn handle_consume() {}

pub async fn try_handshake(
    framed: &mut Framed<TcpStream, LinesCodec>,
) -> Result<Handshake, HandshakeError> {
    let data = framed
        .try_next()
        .await?
        .ok_or(HandshakeError::EmptyMessage)?;

    let data = serde_json::from_str::<Handshake>(&data)?;

    Ok(data)
}

#[tokio::test]
async fn try_handshake_test() {
    let data = r#"{"topic": "test-topic", "mode": "testing"}"#.to_owned();
    let data = serde_json::from_str::<Handshake>(&data);
}

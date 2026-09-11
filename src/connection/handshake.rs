use crate::connection::Client;
use crate::Result;

pub const HANDSHAKE_INTRO: &[u8]  = b"HANDSHAKE";
pub const HANDSHAKE_ACK: &[u8]    = b"ACK";
pub const HANDSHAKE_REJECT: &[u8] = b"REJECT";

pub struct Handshake<'a> {
    client: &'a mut Client,
}

impl<'a> Handshake<'a> {
    pub fn new(client: &'a mut Client) -> Self {
        Self { client }
    }

    pub async fn agree(&mut self) -> Result<bool> {
        let (_, payload) = match self.client.read().await {
            Ok(res) => res,
            Err(_) => return Ok(false),
        };

        if payload.as_ref() != HANDSHAKE_INTRO {
            let _ = self
                .client
                .write(HANDSHAKE_REJECT)
                .await?;
            return Ok(false);
        }

        self
            .client
            .write(HANDSHAKE_ACK)
            .await?;

        Ok(true)
    }
}
mod decoder;
mod encoder;
mod topic;
mod types;

pub use topic::{Topic, TopicFilter};
pub use types::*;

use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};

pub struct MqttCodec;

impl Decoder for MqttCodec {
    type Error = DecodeError;
    type Item = Packet;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let packet = decoder::decode_mqtt(buf)?;
        Ok(packet)
    }
}

impl Encoder<Packet> for MqttCodec {
    type Error = EncodeError;

    fn encode(&mut self, packet: Packet, bytes: &mut BytesMut) -> Result<(), Self::Error> {
        encoder::encode_mqtt(&packet, bytes);
        Ok(())
    }
}

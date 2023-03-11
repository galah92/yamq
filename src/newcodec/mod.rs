pub mod decoder;
pub mod encoder;
pub mod topic;
pub mod types;

pub const TOPIC_SEPARATOR: char = '/';

pub const MULTI_LEVEL_WILDCARD: char = '#';
pub const MULTI_LEVEL_WILDCARD_STR: &str = "#";

pub const SINGLE_LEVEL_WILDCARD: char = '+';
pub const SINGLE_LEVEL_WILDCARD_STR: &str = "+";

pub const SHARED_SUBSCRIPTION_PREFIX: &str = "$share/";

pub const MAX_TOPIC_LEN_BYTES: usize = 65_535;

pub mod codec {
    use super::{
        decoder, encoder,
        types::{DecodeError, EncodeError, Packet, Protocol},
    };
    use bytes::BytesMut;
    use tokio_util::codec::{Decoder, Encoder};

    pub struct MqttCodec {
        version: Protocol,
    }

    impl Default for MqttCodec {
        fn default() -> Self {
            MqttCodec::new()
        }
    }

    impl MqttCodec {
        pub fn new() -> Self {
            MqttCodec {
                version: Protocol::V311,
            }
        }
    }

    impl Decoder for MqttCodec {
        type Error = DecodeError;
        type Item = Packet;

        fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
            let packet = decoder::decode_mqtt(buf)?;
            if let Some(Packet::Connect(packet)) = &packet {
                self.version = packet.protocol;
            }
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
}

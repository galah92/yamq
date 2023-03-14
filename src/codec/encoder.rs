use super::types::{
    Connack, Connect, Packet, Puback, Pubcomp, Publish, Pubrec, Pubrel, Suback, Subscribe,
    Unsuback, Unsubscribe, VariableByteInt,
};
use bytes::{BufMut, BytesMut};

fn encode_variable_int(value: u32, bytes: &mut BytesMut) -> usize {
    let mut x = value;
    let mut byte_counter = 0;

    loop {
        let mut encoded_byte: u8 = (x % 128) as u8;
        x /= 128;

        if x > 0 {
            encoded_byte |= 128;
        }

        bytes.put_u8(encoded_byte);

        byte_counter += 1;

        if x == 0 {
            break;
        }
    }

    byte_counter
}

fn put_delimited_u16(bytes: &mut BytesMut, value: &[u8]) {
    bytes.put_u16(value.len() as u16);
    bytes.put_slice(value);
}

fn encode_connect(packet: &Connect, bytes: &mut BytesMut) {
    put_delimited_u16(bytes, "MQTT".as_bytes());
    bytes.put_u8(packet.protocol as u8);

    let mut connect_flags: u8 = 0b0000_0000;

    if packet.username.is_some() {
        connect_flags |= 0b1000_0000;
    }

    if packet.password.is_some() {
        connect_flags |= 0b0100_0000;
    }

    if let Some(will) = &packet.will {
        if will.should_retain {
            connect_flags |= 0b0100_0000;
        }

        let qos_byte: u8 = will.qos as u8;
        connect_flags |= (qos_byte & 0b0000_0011) << 3;
        connect_flags |= 0b0000_0100;
    }

    if packet.clean_session {
        connect_flags |= 0b0000_0010;
    }

    bytes.put_u8(connect_flags);
    bytes.put_u16(packet.keep_alive);

    put_delimited_u16(bytes, packet.client_id.as_bytes());

    if let Some(will) = &packet.will {
        put_delimited_u16(bytes, will.topic.as_bytes());
        put_delimited_u16(bytes, &will.payload);
    }

    if let Some(user_name) = &packet.username {
        put_delimited_u16(bytes, user_name.as_bytes());
    }

    if let Some(password) = &packet.password {
        put_delimited_u16(bytes, password.as_bytes());
    }
}

fn encode_connect_ack(packet: &Connack, bytes: &mut BytesMut) {
    let mut connect_ack_flags: u8 = 0b0000_0000;
    if packet.session_present {
        connect_ack_flags |= 0b0000_0001;
    }

    bytes.put_u8(connect_ack_flags);
    bytes.put_u8(packet.code as u8);
}

fn encode_publish(packet: &Publish, bytes: &mut BytesMut) {
    put_delimited_u16(bytes, packet.topic.as_bytes());

    if let Some(packet_id) = packet.pid {
        bytes.put_u16(packet_id);
    }

    bytes.put_slice(&packet.payload);
}

fn encode_publish_ack(packet: &Puback, bytes: &mut BytesMut) {
    bytes.put_u16(packet.pid);
}

fn encode_publish_received(packet: &Pubrec, bytes: &mut BytesMut) {
    bytes.put_u16(packet.pid);
}

fn encode_publish_release(packet: &Pubrel, bytes: &mut BytesMut) {
    bytes.put_u16(packet.pid);
}

fn encode_publish_complete(packet: &Pubcomp, bytes: &mut BytesMut) {
    bytes.put_u16(packet.pid);
}

fn encode_subscribe(packet: &Subscribe, bytes: &mut BytesMut) {
    bytes.put_u16(packet.pid);

    for topic in &packet.subscription_topics {
        put_delimited_u16(bytes, topic.topic_path.as_bytes());

        let qos_byte = (topic.qos as u8) & 0b0000_0011;
        bytes.put_u8(qos_byte);
    }
}

fn encode_subscribe_ack(packet: &Suback, bytes: &mut BytesMut) {
    bytes.put_u16(packet.pid);

    for code in &packet.return_codes {
        bytes.put_u8((*code) as u8);
    }
}

fn encode_unsubscribe(packet: &Unsubscribe, bytes: &mut BytesMut) {
    bytes.put_u16(packet.pid);

    for topic in &packet.topics {
        put_delimited_u16(bytes, topic.as_bytes());
    }
}

fn encode_unsubscribe_ack(packet: &Unsuback, bytes: &mut BytesMut) {
    bytes.put_u16(packet.pid);
}

pub fn encode_mqtt(packet: &Packet, bytes: &mut BytesMut) {
    let remaining_length = packet.calculate_size();
    let packet_size = 1 + VariableByteInt(remaining_length).calculate_size() + remaining_length;
    bytes.reserve(packet_size as usize);

    let first_byte = packet.to_byte();
    let mut first_byte_val = (first_byte << 4) & 0b1111_0000;
    first_byte_val |= packet.fixed_header_flags();

    bytes.put_u8(first_byte_val);
    encode_variable_int(remaining_length, bytes);

    match packet {
        Packet::Connect(p) => encode_connect(p, bytes),
        Packet::Connack(p) => encode_connect_ack(p, bytes),
        Packet::Publish(p) => encode_publish(p, bytes),
        Packet::Puback(p) => encode_publish_ack(p, bytes),
        Packet::Pubrec(p) => encode_publish_received(p, bytes),
        Packet::Pubrel(p) => encode_publish_release(p, bytes),
        Packet::Pubcomp(p) => encode_publish_complete(p, bytes),
        Packet::Subscribe(p) => encode_subscribe(p, bytes),
        Packet::Suback(p) => encode_subscribe_ack(p, bytes),
        Packet::Unsubscribe(p) => encode_unsubscribe(p, bytes),
        Packet::Unsuback(p) => encode_unsubscribe_ack(p, bytes),
        Packet::Pingreq => (),
        Packet::Pingresp => (),
        Packet::Disconnect => (),
    }
}

#[cfg(test)]
mod tests {
    use crate::codec::Topic;

    use super::super::{decoder::*, encoder::*, types::*};
    use bytes::BytesMut;

    #[test]
    fn connect_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let packet = Packet::Connect(Connect {
            protocol: Protocol::V311,
            clean_session: true,
            keep_alive: 200,

            client_id: "test_client".to_string(),
            will: None,
            username: None,
            password: None,
        });

        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes);
        let decoded = decode_mqtt(&mut bytes)?;

        assert_eq!(Some(packet), decoded);
        Ok(())
    }

    #[test]
    fn connect_ack_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let packet = Packet::Connack(Connack {
            session_present: false,
            code: ConnectCode::Accepted,
        });

        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes);
        let decoded = decode_mqtt(&mut bytes)?;

        assert_eq!(Some(packet), decoded);
        Ok(())
    }

    #[test]
    fn publish_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let packet = Packet::Publish(Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,

            topic: "test_topic".parse()?,
            pid: Some(42),

            payload: vec![22; 100].into(),
        });

        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes);
        let decoded = decode_mqtt(&mut bytes)?;

        assert_eq!(Some(packet), decoded);
        Ok(())
    }

    #[test]
    fn publish_ack_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let packet = Packet::Puback(Puback { pid: 1500 });

        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes);
        let decoded = decode_mqtt(&mut bytes)?;

        assert_eq!(Some(packet), decoded);
        Ok(())
    }

    #[test]
    fn publish_received_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let packet = Packet::Pubrec(Pubrec { pid: 1500 });

        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes);
        let decoded = decode_mqtt(&mut bytes)?;

        assert_eq!(Some(packet), decoded);
        Ok(())
    }

    #[test]
    fn publish_release_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let packet = Packet::Pubrel(Pubrel { pid: 1500 });

        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes);
        let decoded = decode_mqtt(&mut bytes)?;

        assert_eq!(Some(packet), decoded);
        Ok(())
    }

    #[test]
    fn publish_complete_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let packet = Packet::Pubcomp(Pubcomp { pid: 1500 });

        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes);
        let decoded = decode_mqtt(&mut bytes)?;

        assert_eq!(Some(packet), decoded);
        Ok(())
    }

    #[test]
    fn subscribe_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let packet = Packet::Subscribe(Subscribe {
            pid: 4500,

            subscription_topics: vec![SubscriptionTopic {
                topic_path: Topic::try_from("test_topic")?,
                topic_filter: "test_topic".to_string().parse()?,
                qos: QoS::AtLeastOnce,
            }],
        });

        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes);
        let decoded = decode_mqtt(&mut bytes)?;

        assert_eq!(Some(packet), decoded);
        Ok(())
    }

    #[test]
    fn subscribe_ack_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let packet = Packet::Suback(Suback {
            pid: 1234,

            return_codes: vec![SubscribeAckReason::GrantedQoSZero],
        });

        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes);
        let decoded = decode_mqtt(&mut bytes)?;

        assert_eq!(Some(packet), decoded);
        Ok(())
    }

    #[test]
    fn unsubscribe_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let packet = Packet::Unsubscribe(Unsubscribe {
            pid: 1234,
            topics: vec!["test_topic".parse()?],
        });

        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes);
        let decoded = decode_mqtt(&mut bytes)?;

        assert_eq!(Some(packet), decoded);
        Ok(())
    }

    #[test]
    fn unsubscribe_ack_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let packet = Packet::Unsuback(Unsuback { pid: 4321 });

        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes);
        let decoded = decode_mqtt(&mut bytes)?;

        assert_eq!(Some(packet), decoded);
        Ok(())
    }

    #[test]
    fn ping_request_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let packet = Packet::Pingreq;
        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes);
        let decoded = decode_mqtt(&mut bytes)?;

        assert_eq!(Some(packet), decoded);
        Ok(())
    }

    #[test]
    fn ping_response_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let packet = Packet::Pingresp;
        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes);
        let decoded = decode_mqtt(&mut bytes)?;

        assert_eq!(Some(packet), decoded);
        Ok(())
    }

    #[test]
    fn disconnect_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let packet = Packet::Disconnect;
        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes);
        let decoded = decode_mqtt(&mut bytes)?;

        assert_eq!(Some(packet), decoded);
        Ok(())
    }
}

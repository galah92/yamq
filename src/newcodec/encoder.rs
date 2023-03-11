use super::types::{
    AuthenticatePacket, ConnectAckPacket, ConnectPacket, Packet, PublishAckPacket,
    PublishCompletePacket, PublishPacket, PublishReceivedPacket, PublishReleasePacket,
    SubscribeAckPacket, SubscribePacket, UnsubscribeAckPacket, UnsubscribePacket, VariableByteInt,
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

fn encode_string(value: &str, bytes: &mut BytesMut) {
    bytes.put_u16(value.len() as u16);
    bytes.put_slice(value.as_bytes());
}

fn encode_binary_data(value: &[u8], bytes: &mut BytesMut) {
    bytes.put_u16(value.len() as u16);
    bytes.put_slice(value);
}

fn encode_connect(packet: &ConnectPacket, bytes: &mut BytesMut) {
    encode_string(&packet.protocol_name, bytes);
    bytes.put_u8(packet.protocol as u8);

    let mut connect_flags: u8 = 0b0000_0000;

    if packet.user_name.is_some() {
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

    if packet.clean_start {
        connect_flags |= 0b0000_0010;
    }

    bytes.put_u8(connect_flags);
    bytes.put_u16(packet.keep_alive);

    encode_string(&packet.client_id, bytes);

    if let Some(will) = &packet.will {
        encode_string(will.topic.topic_name(), bytes);
        encode_binary_data(&will.payload, bytes);
    }

    if let Some(user_name) = &packet.user_name {
        encode_string(user_name, bytes);
    }

    if let Some(password) = &packet.password {
        encode_string(password, bytes);
    }
}

fn encode_connect_ack(packet: &ConnectAckPacket, bytes: &mut BytesMut) {
    let mut connect_ack_flags: u8 = 0b0000_0000;
    if packet.session_present {
        connect_ack_flags |= 0b0000_0001;
    }

    bytes.put_u8(connect_ack_flags);
    bytes.put_u8(packet.reason_code as u8);
}

fn encode_publish(packet: &PublishPacket, bytes: &mut BytesMut) {
    encode_string(&packet.topic.to_string(), bytes);

    if let Some(packet_id) = packet.packet_id {
        bytes.put_u16(packet_id);
    }

    bytes.put_slice(&packet.payload);
}

fn encode_publish_ack(packet: &PublishAckPacket, bytes: &mut BytesMut) {
    bytes.put_u16(packet.packet_id);
}

fn encode_publish_received(packet: &PublishReceivedPacket, bytes: &mut BytesMut) {
    bytes.put_u16(packet.packet_id);
}

fn encode_publish_release(packet: &PublishReleasePacket, bytes: &mut BytesMut) {
    bytes.put_u16(packet.packet_id);
}

fn encode_publish_complete(packet: &PublishCompletePacket, bytes: &mut BytesMut) {
    bytes.put_u16(packet.packet_id);
}

fn encode_subscribe(packet: &SubscribePacket, bytes: &mut BytesMut) {
    bytes.put_u16(packet.packet_id);

    for topic in &packet.subscription_topics {
        encode_string(&topic.topic_filter.to_string(), bytes);

        let mut options_byte = 0b0000_0000;
        let retain_handling_byte = topic.retain_handling as u8;
        options_byte |= (retain_handling_byte & 0b0000_0011) << 4;

        if topic.retain_as_published {
            options_byte |= 0b0000_1000;
        }

        if topic.no_local {
            options_byte |= 0b0000_0100;
        }

        let qos_byte = topic.maximum_qos as u8;
        options_byte |= qos_byte & 0b0000_0011;

        bytes.put_u8(options_byte);
    }
}

fn encode_subscribe_ack(packet: &SubscribeAckPacket, bytes: &mut BytesMut) {
    bytes.put_u16(packet.packet_id);

    for code in &packet.reason_codes {
        bytes.put_u8((*code) as u8);
    }
}

fn encode_unsubscribe(packet: &UnsubscribePacket, bytes: &mut BytesMut) {
    bytes.put_u16(packet.packet_id);

    for topic_filter in &packet.topic_filters {
        encode_string(&topic_filter.to_string(), bytes);
    }
}

fn encode_unsubscribe_ack(packet: &UnsubscribeAckPacket, bytes: &mut BytesMut) {
    bytes.put_u16(packet.packet_id);

    for code in &packet.reason_codes {
        bytes.put_u8((*code) as u8);
    }
}

fn encode_authenticate(packet: &AuthenticatePacket, bytes: &mut BytesMut) {
    bytes.put_u8(packet.reason_code as u8);
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
        Packet::ConnectAck(p) => encode_connect_ack(p, bytes),
        Packet::Publish(p) => encode_publish(p, bytes),
        Packet::PublishAck(p) => encode_publish_ack(p, bytes),
        Packet::PublishReceived(p) => encode_publish_received(p, bytes),
        Packet::PublishRelease(p) => encode_publish_release(p, bytes),
        Packet::PublishComplete(p) => encode_publish_complete(p, bytes),
        Packet::Subscribe(p) => encode_subscribe(p, bytes),
        Packet::SubscribeAck(p) => encode_subscribe_ack(p, bytes),
        Packet::Unsubscribe(p) => encode_unsubscribe(p, bytes),
        Packet::UnsubscribeAck(p) => encode_unsubscribe_ack(p, bytes),
        Packet::PingRequest => {}
        Packet::PingResponse => {}
        Packet::Disconnect => {}
        Packet::Authenticate(p) => encode_authenticate(p, bytes),
    }
}

#[cfg(test)]
mod tests {
    use super::super::{decoder::*, encoder::*, types::*};
    use bytes::BytesMut;

    #[test]
    fn connect_roundtrip() {
        let packet = Packet::Connect(ConnectPacket {
            protocol_name: "MQTT".to_string(),
            protocol: Protocol::V311,
            clean_start: true,
            keep_alive: 200,

            client_id: "test_client".to_string(),
            will: None,
            user_name: None,
            password: None,
        });

        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes);
        let decoded = decode_mqtt(&mut bytes).unwrap().unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn connect_ack_roundtrip() {
        let packet = Packet::ConnectAck(ConnectAckPacket {
            session_present: false,
            reason_code: ConnectReason::Success,
        });

        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes);
        let decoded = decode_mqtt(&mut bytes).unwrap().unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn publish_roundtrip() {
        let packet = Packet::Publish(PublishPacket {
            is_duplicate: false,
            qos: QoS::AtLeastOnce,
            retain: false,

            topic: "test_topic".parse().unwrap(),
            packet_id: Some(42),

            payload: vec![22; 100].into(),
        });

        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes);
        let decoded = decode_mqtt(&mut bytes).unwrap().unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn publish_ack_roundtrip() {
        let packet = Packet::PublishAck(PublishAckPacket {
            packet_id: 1500,
            reason_code: PublishAckReason::Success,
        });

        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes);
        let decoded = decode_mqtt(&mut bytes).unwrap().unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn publish_received_roundtrip() {
        let packet = Packet::PublishReceived(PublishReceivedPacket {
            packet_id: 1500,
            reason_code: PublishReceivedReason::Success,
        });

        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes);
        let decoded = decode_mqtt(&mut bytes).unwrap().unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn publish_release_roundtrip() {
        let packet = Packet::PublishRelease(PublishReleasePacket {
            packet_id: 1500,
            reason_code: PublishReleaseReason::Success,
        });

        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes);
        let decoded = decode_mqtt(&mut bytes).unwrap().unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn publish_complete_roundtrip() {
        let packet = Packet::PublishComplete(PublishCompletePacket {
            packet_id: 1500,
            reason_code: PublishCompleteReason::Success,
        });

        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes);
        let decoded = decode_mqtt(&mut bytes).unwrap().unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn subscribe_roundtrip() {
        let packet = Packet::Subscribe(SubscribePacket {
            packet_id: 4500,

            subscription_topics: vec![SubscriptionTopic {
                topic_filter: "test_topic".parse().unwrap(),
                maximum_qos: QoS::AtLeastOnce,
                no_local: false,
                retain_as_published: false,
                retain_handling: RetainHandling::SendAtSubscribeTime,
            }],
        });

        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes);
        let decoded = decode_mqtt(&mut bytes).unwrap().unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn subscribe_ack_roundtrip() {
        let packet = Packet::SubscribeAck(SubscribeAckPacket {
            packet_id: 1234,

            reason_codes: vec![SubscribeAckReason::GrantedQoSZero],
        });

        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes);
        let decoded = decode_mqtt(&mut bytes).unwrap().unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn unsubscribe_roundtrip() {
        let packet = Packet::Unsubscribe(UnsubscribePacket {
            packet_id: 1234,
            topic_filters: vec!["test_topic".parse().unwrap()],
        });

        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes);
        let decoded = decode_mqtt(&mut bytes).unwrap().unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn unsubscribe_ack_roundtrip() {
        let packet = Packet::UnsubscribeAck(UnsubscribeAckPacket {
            packet_id: 4321,

            reason_codes: vec![UnsubscribeAckReason::Success],
        });

        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes);
        let decoded = decode_mqtt(&mut bytes).unwrap().unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn ping_request_roundtrip() {
        let packet = Packet::PingRequest;
        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes);
        let decoded = decode_mqtt(&mut bytes).unwrap().unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn ping_response_roundtrip() {
        let packet = Packet::PingResponse;
        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes);
        let decoded = decode_mqtt(&mut bytes).unwrap().unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn disconnect_roundtrip() {
        let packet = Packet::Disconnect;
        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes);
        let decoded = decode_mqtt(&mut bytes).unwrap().unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn authenticate_roundtrip() {
        let packet = Packet::Authenticate(AuthenticatePacket {
            reason_code: AuthenticateReason::Success,
        });
        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes);
        let decoded = decode_mqtt(&mut bytes).unwrap().unwrap();

        assert_eq!(packet, decoded);
    }
}

use super::topic::{Topic, TopicFilter, TopicParseError};
use bytes::{Bytes, BytesMut};
use num_enum::TryFromPrimitive;

#[derive(Debug)]
pub enum DecodeError {
    InvalidPacketType,
    InvalidProtocolVersion,
    InvalidRemainingLength,
    InvalidUtf8,
    InvalidQoS,
    InvalidConnectReason,
    InvalidSubscribeAckReason,
    InvalidUnsubscribeAckReason,
    InvalidTopic(TopicParseError),
    InvalidTopicFilter(TopicParseError),
    Io(std::io::Error),
}

#[derive(Debug)]
pub enum EncodeError {
    Io(std::io::Error),
}

#[repr(u8)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, TryFromPrimitive)]
pub enum Protocol {
    V311 = 4,
    V500 = 5,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VariableByteInt(pub u32);

impl VariableByteInt {
    pub fn calculate_size(&self) -> u32 {
        self.calc_size()
    }
}

impl From<std::io::Error> for DecodeError {
    fn from(err: std::io::Error) -> Self {
        DecodeError::Io(err)
    }
}

impl From<std::io::Error> for EncodeError {
    fn from(err: std::io::Error) -> Self {
        EncodeError::Io(err)
    }
}

trait PacketSize {
    fn calc_size(&self) -> u32;
}

trait Encode {
    fn encode(&self, bytes: &mut BytesMut);
}

impl<T: Encode> Encode for Option<T> {
    fn encode(&self, bytes: &mut BytesMut) {
        if let Some(data) = self {
            data.encode(bytes);
        }
    }
}

impl PacketSize for u16 {
    fn calc_size(&self) -> u32 {
        2
    }
}

impl PacketSize for VariableByteInt {
    fn calc_size(&self) -> u32 {
        match self.0 {
            0..=127 => 1,
            128..=16_383 => 2,
            16384..=2_097_151 => 3,
            2_097_152..=268_435_455 => 4,
            _ => unreachable!(),
        }
    }
}

impl PacketSize for String {
    fn calc_size(&self) -> u32 {
        2 + self.len() as u32
    }
}

impl PacketSize for &str {
    fn calc_size(&self) -> u32 {
        2 + self.len() as u32
    }
}

impl PacketSize for &[u8] {
    fn calc_size(&self) -> u32 {
        2 + self.len() as u32
    }
}

impl PacketSize for Bytes {
    fn calc_size(&self) -> u32 {
        2 + self.len() as u32
    }
}

impl PacketSize for Vec<SubscriptionTopic> {
    fn calc_size(&self) -> u32 {
        self.iter().map(|x| x.calc_size()).sum()
    }
}

impl PacketSize for Vec<String> {
    fn calc_size(&self) -> u32 {
        self.iter().map(|x| x.calc_size()).sum()
    }
}

impl<T: PacketSize> PacketSize for Option<T> {
    fn calc_size(&self) -> u32 {
        match self {
            Some(p) => p.calc_size(),
            None => 0,
        }
    }
}

impl PacketSize for Topic {
    fn calc_size(&self) -> u32 {
        let topic: &str = self;
        topic.calc_size()
    }
}

impl PacketSize for Vec<Topic> {
    fn calc_size(&self) -> u32 {
        self.iter().map(|x| x.calc_size()).sum()
    }
}

impl PacketSize for TopicFilter {
    fn calc_size(&self) -> u32 {
        0
    }
}

impl PacketSize for Vec<TopicFilter> {
    fn calc_size(&self) -> u32 {
        self.iter().map(|x| x.calc_size()).sum()
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, TryFromPrimitive)]
#[allow(clippy::enum_variant_names)]
pub enum QoS {
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExactlyOnce = 2,
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, TryFromPrimitive)]
pub enum RetainHandling {
    SendAtSubscribeTime = 0,
    SendAtSubscribeTimeIfNonexistent = 1,
    DoNotSend = 2,
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, TryFromPrimitive)]
pub enum ConnectCode {
    /// Connection accepted
    Accepted = 0x00,
    /// The Server does not support the level of the MQTT protocol requested by the Client
    UnacceptableProtocol = 0x01,
    /// The Client identifier is correct UTF-8 but not allowed by the Server
    IdentifierRejected = 0x02,
    /// The Network Connection has been made but the MQTT service is unavailable
    ServerUnavailable = 0x03,
    /// The data in the user name or password is malformed
    BadUsernameOrPassword = 0x04,
    /// The Client is not authorized to connect
    NotAuthorized = 0x05,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, TryFromPrimitive)]
pub enum SubscribeAckReason {
    GrantedQoSZero = 0,
    GrantedQoSOne = 1,
    GrantedQoSTwo = 2,
    UnspecifiedError = 128,
    ImplementationSpecificError = 131,
    NotAuthorized = 135,
    TopicFilterInvalid = 143,
    PacketIdentifierInUse = 145,
    QuotaExceeded = 151,
    SharedSubscriptionsNotSupported = 158,
    SubscriptionIdentifiersNotSupported = 161,
    WildcardSubscriptionsNotSupported = 162,
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, TryFromPrimitive)]
pub enum UnsubscribeAckReason {
    Success = 0,
    NoSubscriptionExisted = 17,
    UnspecifiedError = 128,
    ImplementationSpecificError = 131,
    NotAuthorized = 135,
    TopicFilterInvalid = 143,
    PacketIdentifierInUse = 145,
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, TryFromPrimitive)]
pub enum DisconnectReason {
    NormalDisconnection = 0,
    DisconnectWithWillMessage = 4,
    UnspecifiedError = 128,
    MalformedPacket = 129,
    ProtocolError = 130,
    ImplementationSpecificError = 131,
    NotAuthorized = 135,
    ServerBusy = 137,
    ServerShuttingDown = 139,
    KeepAliveTimeout = 141,
    SessionTakenOver = 142,
    TopicFilterInvalid = 143,
    TopicNameInvalid = 144,
    ReceiveMaximumExceeded = 147,
    TopicAliasInvalid = 148,
    PacketTooLarge = 149,
    MessageRateTooHigh = 150,
    QuotaExceeded = 151,
    AdministrativeAction = 152,
    PayloadFormatInvalid = 153,
    RetainNotSupported = 154,
    QosNotSupported = 155,
    UseAnotherServer = 156,
    ServerMoved = 157,
    SharedSubscriptionNotAvailable = 158,
    ConnectionRateExceeded = 159,
    MaximumConnectTime = 160,
    SubscriptionIdentifiersNotAvailable = 161,
    WildcardSubscriptionsNotAvailable = 162,
}

// Payloads
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalWill {
    pub topic: Topic,
    pub payload: Bytes,
    pub qos: QoS,
    pub should_retain: bool,
}

impl PacketSize for FinalWill {
    fn calc_size(&self) -> u32 {
        let mut size = 0;

        size += self.topic.calc_size();
        size += self.payload.calc_size();

        size
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionTopic {
    pub topic_path: String,
    pub topic_filter: TopicFilter,
    pub qos: QoS,
}

impl PacketSize for SubscriptionTopic {
    fn calc_size(&self) -> u32 {
        self.topic_path.calc_size() + self.topic_filter.calc_size() + 1
    }
}

// Control Packets
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Connect {
    pub protocol_name: String,
    pub protocol: Protocol,
    pub clean_session: bool,
    pub client_id: String,
    pub keep_alive: u16,
    pub will: Option<FinalWill>,
    pub username: Option<String>,
    pub password: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Connack {
    pub session_present: bool,
    pub code: ConnectCode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Publish {
    pub dup: bool,
    pub qos: QoS,
    pub pid: Option<u16>, // TODO: should be conditional on QoS
    pub retain: bool,
    pub topic: Topic,
    pub payload: Bytes,
}

impl From<FinalWill> for Publish {
    fn from(will: FinalWill) -> Self {
        Self {
            dup: false,
            qos: will.qos,
            retain: will.should_retain,

            // Variable header
            topic: will.topic,
            pid: None,

            // Payload
            payload: will.payload,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Puback {
    pub pid: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pubrec {
    pub pid: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pubrel {
    pub pid: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pubcomp {
    pub pid: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subscribe {
    pub pid: u16,
    pub subscription_topics: Vec<SubscriptionTopic>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Suback {
    pub pid: u16,
    pub return_codes: Vec<SubscribeAckReason>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unsubscribe {
    pub pid: u16,
    pub topics: Vec<Topic>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unsuback {
    pub pid: u16,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Packet {
    Connect(Connect),
    Connack(Connack),
    Publish(Publish),
    Puback(Puback),
    Pubrec(Pubrec),
    Pubrel(Pubrel),
    Pubcomp(Pubcomp),
    Subscribe(Subscribe),
    Suback(Suback),
    Unsubscribe(Unsubscribe),
    Unsuback(Unsuback),
    Pingreq,
    Pingresp,
    Disconnect,
}

impl Packet {
    pub fn to_byte(&self) -> u8 {
        match self {
            Packet::Connect(_) => 1,
            Packet::Connack(_) => 2,
            Packet::Publish(_) => 3,
            Packet::Puback(_) => 4,
            Packet::Pubrec(_) => 5,
            Packet::Pubrel(_) => 6,
            Packet::Pubcomp(_) => 7,
            Packet::Subscribe(_) => 8,
            Packet::Suback(_) => 9,
            Packet::Unsubscribe(_) => 10,
            Packet::Unsuback(_) => 11,
            Packet::Pingreq => 12,
            Packet::Pingresp => 13,
            Packet::Disconnect => 14,
        }
    }

    pub fn fixed_header_flags(&self) -> u8 {
        match self {
            Packet::Connect(_)
            | Packet::Connack(_)
            | Packet::Puback(_)
            | Packet::Pubrec(_)
            | Packet::Pubcomp(_)
            | Packet::Suback(_)
            | Packet::Unsuback(_)
            | Packet::Pingreq
            | Packet::Pingresp
            | Packet::Disconnect
            | Packet::Pubrel(_)
            | Packet::Subscribe(_)
            | Packet::Unsubscribe(_) => 0b0000_0010,
            Packet::Publish(publish_packet) => {
                let mut flags: u8 = 0;

                if publish_packet.dup {
                    flags |= 0b0000_1000;
                }

                let qos = publish_packet.qos as u8;
                let qos_bits = 0b0000_0110 & (qos << 1);
                flags |= qos_bits;

                if publish_packet.retain {
                    flags |= 0b0000_0001;
                }

                flags
            }
        }
    }

    pub fn calculate_size(&self) -> u32 {
        self.calc_size()
    }
}

impl PacketSize for Packet {
    fn calc_size(&self) -> u32 {
        match self {
            Packet::Connect(p) => {
                let mut size = p.protocol_name.calc_size();

                // Protocol level + connect flags + keep-alive
                size += 1 + 1 + 2;

                size += p.client_id.calc_size();
                size += p.will.calc_size();
                size += p.username.calc_size();
                size += p.password.calc_size();

                size
            }
            Packet::Connack(_p) => {
                // flags + reason code
                1 + 1
            }
            Packet::Publish(p) => {
                let mut size = p.topic.calc_size();
                size += p.pid.calc_size();

                // This payload does not have a length prefix
                size += p.payload.len() as u32;

                size
            }
            Packet::Puback(_p) => {
                // packet_id

                2
            }
            Packet::Pubrec(_p) => {
                // packet_id

                2
            }
            Packet::Pubrel(_p) => {
                // packet_id

                2
            }
            Packet::Pubcomp(_p) => {
                // packet_id

                2
            }
            Packet::Subscribe(p) => {
                // packet_id
                let mut size = 2;

                size += p.subscription_topics.calc_size();

                size
            }
            Packet::Suback(p) => {
                // Packet id
                let mut size = 2;

                size += p.return_codes.len() as u32;

                size
            }
            Packet::Unsubscribe(p) => {
                // Packet id
                let mut size = 2;

                size += p.topics.calc_size();

                size
            }
            Packet::Unsuback(_p) => {
                // Packet id

                2
            }
            Packet::Pingreq => 0,
            Packet::Pingresp => 0,
            Packet::Disconnect => 0,
        }
    }
}

use super::{
    topic::{Topic, TopicFilter, TopicParseError},
    SHARED_SUBSCRIPTION_PREFIX,
};
use bytes::{Bytes, BytesMut};
use num_enum::TryFromPrimitive;

#[derive(Debug)]
pub enum DecodeError {
    InvalidPacketType,
    InvalidProtocolVersion,
    InvalidRemainingLength,
    InvalidUtf8,
    InvalidQoS,
    InvalidRetainHandling,
    InvalidConnectReason,
    InvalidPublishAckReason,
    InvalidPublishReceivedReason,
    InvalidPublishReleaseReason,
    InvalidPublishCompleteReason,
    InvalidSubscribeAckReason,
    InvalidUnsubscribeAckReason,
    InvalidAuthenticateReason,
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

pub trait Encode {
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
        self.topic_name().calc_size()
    }
}

impl PacketSize for TopicFilter {
    fn calc_size(&self) -> u32 {
        match self {
            TopicFilter::Concrete { filter, .. } | TopicFilter::Wildcard { filter, .. } => {
                filter.calc_size()
            }
            TopicFilter::SharedConcrete {
                group_name, filter, ..
            }
            | TopicFilter::SharedWildcard {
                group_name, filter, ..
            } => {
                (2 + SHARED_SUBSCRIPTION_PREFIX.len() + group_name.len() + 1 + filter.len()) as u32
            }
        }
    }
}

impl PacketSize for Vec<TopicFilter> {
    fn calc_size(&self) -> u32 {
        self.iter().map(|x| x.calc_size()).sum()
    }
}

#[repr(u8)]
#[derive(Debug, TryFromPrimitive)]
pub enum PacketType {
    Connect = 1,
    ConnectAck = 2,
    Publish = 3,
    PublishAck = 4,
    PublishReceived = 5,
    PublishRelease = 6,
    PublishComplete = 7,
    Subscribe = 8,
    SubscribeAck = 9,
    Unsubscribe = 10,
    UnsubscribeAck = 11,
    PingRequest = 12,
    PingResponse = 13,
    Disconnect = 14,
    Authenticate = 15,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, TryFromPrimitive)]
#[allow(clippy::enum_variant_names)]
pub enum QoS {
    AtMostOnce = 0,  // QoS 0
    AtLeastOnce = 1, // QoS 1
    ExactlyOnce = 2, // QoS 2
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
pub enum ConnectReason {
    Success = 0,
    UnspecifiedError = 128,
    MalformedPacket = 129,
    ProtocolError = 130,
    ImplementationSpecificError = 131,
    UnsupportedProtocolVersion = 132,
    ClientIdentifierNotValid = 133,
    BadUserNameOrPassword = 134,
    NotAuthorized = 135,
    ServerUnavailable = 136,
    ServerBusy = 137,
    Banned = 138,
    BadAuthenticationMethod = 140,
    TopicNameInvalid = 144,
    PacketTooLarge = 149,
    QuotaExceeded = 151,
    PayloadFormatInvalid = 153,
    RetainNotSupported = 154,
    QosNotSupported = 155,
    UseAnotherServer = 156,
    ServerMoved = 157,
    ConnectionRateExceeded = 159,
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, TryFromPrimitive)]
pub enum PublishAckReason {
    Success = 0,
    NoMatchingSubscribers = 16,
    UnspecifiedError = 128,
    ImplementationSpecificError = 131,
    NotAuthorized = 135,
    TopicNameInvalid = 144,
    PacketIdentifierInUse = 145,
    QuotaExceeded = 151,
    PayloadFormatInvalid = 153,
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, TryFromPrimitive)]
pub enum PublishReceivedReason {
    Success = 0,
    NoMatchingSubscribers = 16,
    UnspecifiedError = 128,
    ImplementationSpecificError = 131,
    NotAuthorized = 135,
    TopicNameInvalid = 144,
    PacketIdentifierInUse = 145,
    QuotaExceeded = 151,
    PayloadFormatInvalid = 153,
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, TryFromPrimitive)]
pub enum PublishReleaseReason {
    Success = 0,
    PacketIdentifierNotFound = 146,
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, TryFromPrimitive)]
pub enum PublishCompleteReason {
    Success = 0,
    PacketIdentifierNotFound = 146,
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

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, TryFromPrimitive)]
pub enum AuthenticateReason {
    Success = 0,
    ContinueAuthentication = 24,
    ReAuthenticate = 25,
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

#[derive(Debug, PartialEq, Eq)]
pub struct SubscriptionTopic {
    pub topic_filter: TopicFilter,
    pub maximum_qos: QoS,
    pub no_local: bool,
    pub retain_as_published: bool,
    pub retain_handling: RetainHandling,
}

impl PacketSize for SubscriptionTopic {
    fn calc_size(&self) -> u32 {
        self.topic_filter.calc_size() + 1
    }
}

// Control Packets
#[derive(Debug, PartialEq, Eq)]
pub struct Connect {
    // Variable Header
    pub protocol_name: String,
    pub protocol: Protocol,
    pub clean_start: bool,
    pub keep_alive: u16,

    // Payload
    pub client_id: String,
    pub will: Option<FinalWill>,
    pub user_name: Option<String>,
    pub password: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Connack {
    // Variable header
    pub session_present: bool,
    pub reason_code: ConnectReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Publish {
    // Fixed header
    pub is_duplicate: bool,
    pub qos: QoS,
    pub retain: bool,

    // Variable header
    pub topic: Topic,
    pub packet_id: Option<u16>,

    // Payload
    pub payload: Bytes,
}

impl From<FinalWill> for Publish {
    fn from(will: FinalWill) -> Self {
        Self {
            is_duplicate: false,
            qos: will.qos,
            retain: will.should_retain,

            // Variable header
            topic: will.topic,
            packet_id: None,

            // Payload
            payload: will.payload,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Puback {
    // Variable header
    pub packet_id: u16,
    pub reason_code: PublishAckReason,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Pubrec {
    // Variable header
    pub packet_id: u16,
    pub reason_code: PublishReceivedReason,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Pubrel {
    // Variable header
    pub packet_id: u16,
    pub reason_code: PublishReleaseReason,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Pubcomp {
    // Variable header
    pub packet_id: u16,
    pub reason_code: PublishCompleteReason,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Subscribe {
    // Variable header
    pub packet_id: u16,

    // Payload
    pub subscription_topics: Vec<SubscriptionTopic>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Suback {
    // Variable header
    pub packet_id: u16,

    // Payload
    pub reason_codes: Vec<SubscribeAckReason>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Unsubscribe {
    // Variable header
    pub packet_id: u16,

    // Payload
    pub topic_filters: Vec<TopicFilter>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Unsuback {
    // Variable header
    pub packet_id: u16,

    // Payload
    pub reason_codes: Vec<UnsubscribeAckReason>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct AuthenticatePacket {
    // Variable header
    pub reason_code: AuthenticateReason,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, PartialEq, Eq)]
pub enum Packet {
    Connect(Connect),
    ConnectAck(Connack),
    Publish(Publish),
    PublishAck(Puback),
    PublishReceived(Pubrec),
    PublishRelease(Pubrel),
    PublishComplete(Pubcomp),
    Subscribe(Subscribe),
    SubscribeAck(Suback),
    Unsubscribe(Unsubscribe),
    UnsubscribeAck(Unsuback),
    PingRequest,
    PingResponse,
    Disconnect,
    Authenticate(AuthenticatePacket),
}

impl Packet {
    pub fn to_byte(&self) -> u8 {
        match self {
            Packet::Connect(_) => 1,
            Packet::ConnectAck(_) => 2,
            Packet::Publish(_) => 3,
            Packet::PublishAck(_) => 4,
            Packet::PublishReceived(_) => 5,
            Packet::PublishRelease(_) => 6,
            Packet::PublishComplete(_) => 7,
            Packet::Subscribe(_) => 8,
            Packet::SubscribeAck(_) => 9,
            Packet::Unsubscribe(_) => 10,
            Packet::UnsubscribeAck(_) => 11,
            Packet::PingRequest => 12,
            Packet::PingResponse => 13,
            Packet::Disconnect => 14,
            Packet::Authenticate(_) => 15,
        }
    }

    pub fn fixed_header_flags(&self) -> u8 {
        match self {
            Packet::Connect(_)
            | Packet::ConnectAck(_)
            | Packet::PublishAck(_)
            | Packet::PublishReceived(_)
            | Packet::PublishComplete(_)
            | Packet::SubscribeAck(_)
            | Packet::UnsubscribeAck(_)
            | Packet::PingRequest
            | Packet::PingResponse
            | Packet::Disconnect
            | Packet::Authenticate(_) => 0b0000_0000,
            Packet::PublishRelease(_) | Packet::Subscribe(_) | Packet::Unsubscribe(_) => {
                0b0000_0010
            }
            Packet::Publish(publish_packet) => {
                let mut flags: u8 = 0;

                if publish_packet.is_duplicate {
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
                size += p.user_name.calc_size();
                size += p.password.calc_size();

                size
            }
            Packet::ConnectAck(_p) => {
                // flags + reason code
                1 + 1
            }
            Packet::Publish(p) => {
                let mut size = p.topic.calc_size();
                size += p.packet_id.calc_size();

                // This payload does not have a length prefix
                size += p.payload.len() as u32;

                size
            }
            Packet::PublishAck(_p) => {
                // packet_id

                2
            }
            Packet::PublishReceived(_p) => {
                // packet_id

                2
            }
            Packet::PublishRelease(_p) => {
                // packet_id

                2
            }
            Packet::PublishComplete(_p) => {
                // packet_id

                2
            }
            Packet::Subscribe(p) => {
                // packet_id
                let mut size = 2;

                size += p.subscription_topics.calc_size();

                size
            }
            Packet::SubscribeAck(p) => {
                // Packet id
                let mut size = 2;

                size += p.reason_codes.len() as u32;

                size
            }
            Packet::Unsubscribe(p) => {
                // Packet id
                let mut size = 2;

                size += p.topic_filters.calc_size();

                size
            }
            Packet::UnsubscribeAck(p) => {
                // Packet id
                let mut size = 2;

                size += p.reason_codes.len() as u32;

                size
            }
            Packet::PingRequest => 0,
            Packet::PingResponse => 0,
            Packet::Disconnect => 0,
            Packet::Authenticate(_p) => {
                // reason_code

                1
            }
        }
    }
}

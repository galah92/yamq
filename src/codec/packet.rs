use super::*;

#[derive(Debug, Clone, PartialEq)]
pub enum Packet<'a> {
    /// [MQTT 3.1](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718028)
    Connect(Connect<'a>),
    /// [MQTT 3.2](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718033)
    Connack(Connack),
    /// [MQTT 3.3](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718037)
    Publish(Publish<'a>),
    /// [MQTT 3.4](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718043)
    Puback(Pid),
    /// [MQTT 3.5](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718048)
    Pubrec(Pid),
    /// [MQTT 3.6](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718053)
    Pubrel(Pid),
    /// [MQTT 3.7](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718058)
    Pubcomp(Pid),
    /// [MQTT 3.8](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718063)
    Subscribe(Subscribe),
    /// [MQTT 3.9](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718068)
    Suback(Suback),
    /// [MQTT 3.10](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718072)
    Unsubscribe(Unsubscribe),
    /// [MQTT 3.11](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718077)
    Unsuback(Pid),
    /// [MQTT 3.12](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718081)
    Pingreq,
    /// [MQTT 3.13](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718086)
    Pingresp,
    /// [MQTT 3.14](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718090)
    Disconnect,
}

macro_rules! packet_from_borrowed {
    ($($t:ident),+) => {
        $(
            impl<'a> From<$t<'a>> for Packet<'a> {
                fn from(p: $t<'a>) -> Self {
                    Packet::$t(p)
                }
            }
        )+
    }
}
macro_rules! packet_from {
    ($($t:ident),+) => {
        $(
            impl<'a> From<$t> for Packet<'a> {
                fn from(p: $t) -> Self {
                    Packet::$t(p)
                }
            }
        )+
    }
}

packet_from_borrowed!(Connect, Publish);
packet_from!(Suback, Connack, Subscribe, Unsubscribe);

/// Packet type variant, without the associated data.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PacketType {
    Connect,
    Connack,
    Publish,
    Puback,
    Pubrec,
    Pubrel,
    Pubcomp,
    Subscribe,
    Suback,
    Unsubscribe,
    Unsuback,
    Pingreq,
    Pingresp,
    Disconnect,
}

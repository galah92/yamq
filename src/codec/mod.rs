#[cfg(feature = "std")]
extern crate std;

mod connect;
mod decoder;
mod encoder;
mod packet;
mod publish;
mod subscribe;
mod utils;

// Proptest does not currently support borrowed data in strategies:
// https://github.com/AltSysrq/proptest/issues/9
//
// #[cfg(test)]
// mod codec_test;
#[cfg(test)]
mod decoder_test;
#[cfg(test)]
mod encoder_test;

pub use {
    connect::{Connack, Connect, ConnectReturnCode, LastWill, Protocol},
    decoder::{decode_slice, decode_slice_with_len},
    encoder::encode_slice,
    packet::{Packet, PacketType},
    publish::Publish,
    subscribe::{Suback, Subscribe, SubscribeReturnCodes, SubscribeTopic, Unsubscribe},
    utils::{Error, Pid, QoS, QosPid},
};

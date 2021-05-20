use derive_more::Display;
use std::io;

/// Errors which can occur when attempting to handle mqtt connection.
#[derive(Debug)]
pub enum MqttError<E> {
    /// Message handler service error
    Service(E),
    /// Mqtt parse error
    Protocol(mqtt_codec::ParseError),
    /// Unexpected packet
    Unexpected(mqtt_codec::Packet, &'static str),
    /// "SUBSCRIBE, UNSUBSCRIBE, and PUBLISH (in cases where QoS > 0) Control Packets MUST contain a non-zero 16-bit Packet Identifier [MQTT-2.3.1-1]."
    PacketIdRequired,
    /// Keep alive timeout
    KeepAliveTimeout,
    /// Peer disconnect
    Disconnected,
    /// Unexpected io error
    Io(io::Error),
}

impl<E> From<mqtt_codec::ParseError> for MqttError<E> {
    fn from(err: mqtt_codec::ParseError) -> Self {
        MqttError::Protocol(err)
    }
}

impl<E> From<io::Error> for MqttError<E> {
    fn from(err: io::Error) -> Self {
        MqttError::Io(err)
    }
}

#[derive(Debug, Display, PartialEq)]
pub enum SendPacketError {
    /// Encoder error
    Encode(EncodeError),
    /// Provided packet id is in use
    #[display(fmt = "Provided packet id is in use")]
    PacketIdInUse(u16),
    /// Peer disconnected
    #[display(fmt = "Peer disconnected")]
    Disconnected,
    #[display(fmt = "Packet id")]
    PacketId,
}

#[derive(Copy, Clone, Debug, Display, PartialEq, Eq, Hash)]
pub enum EncodeError {
    InvalidLength,
    MalformedPacket,
    PacketIdRequired,
    UnsupportedVersion,
}

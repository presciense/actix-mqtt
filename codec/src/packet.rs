use bytes::Bytes;
use std::num::{NonZeroU16, NonZeroU32};
use string::String;

use crate::proto::{Protocol, QoS};

#[repr(u8)]
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
/// CONNACK reason codes
pub enum ConnectAckReasonCode {
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
const_enum!(ConnectAckReasonCode: u8);

impl ConnectAckReasonCode {
    pub fn reason(self) -> &'static str {
        match self {
            ConnectAckReasonCode::Success => "Connection Accepted",
            ConnectAckReasonCode::UnsupportedProtocolVersion => {
                "protocol version is not supported"
            }
            ConnectAckReasonCode::ClientIdentifierNotValid => "client identifier is invalid",
            ConnectAckReasonCode::ServerUnavailable => "Server unavailable",
            ConnectAckReasonCode::BadUserNameOrPassword => "bad user name or password",
            ConnectAckReasonCode::NotAuthorized => "not authorized",
            _ => "Connection Refused",
        }
    }
}

/// DISCONNECT reason codes
pub enum DisconnectReasonCode {
    NormalDisconnection = 0,
    DisconnectWithWillMessage = 4,
    UnspecifiedError = 128,
    MalformedPacket = 129,
    ProtocolError = 130,
    ImplementationSpecificError = 131,
    NotAuthorized = 135,
    ServerBusy = 137,
    ServerShuttingDown = 139,
    BadAuthenticationMethod = 140,
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
    SharedSubsriptionNotSupported = 158,
    ConnectionRateExceeded = 159,
    MaximumConnectTime = 160,
    SubscriptionIdentifiersNotSupported = 161,
    WildcardSubscriptionsNotSupported = 162,
}
const_enum!(DisconnectReasonCode: u8);

/// SUBACK reason codes
pub enum SubscribeAckReasonCode {
    GrantedQos0 = 0,
    GrantedQos1 = 1,
    GrantedQos2 = 2,
    UnspecifiedError = 128,
    ImplementationSpecificError = 131,
    NotAuthorized = 135,
    TopicFilterInvalid = 143,
    PacketIdentifierInUse = 145,
    QuotaExceeded = 151,
    SharedSubsriptionNotSupported = 158,
    SubscriptionIdentifiersNotSupported = 161,
    WildcardSubscriptionsNotSupported = 162,
}
const_enum!(SubscribeAckReasonCode: u8);

/// PUBACK / PUBREC reason codes
pub enum PublishAckReasonCode {
    Success = 0,
    NoMatchingSubscribers = 16,
    UnspecifiedError = 128,
    ImplementationSpecificError = 131,
    NotAuthorized = 135,
    ServerBusy = 137,
    ServerShuttingDown = 139,
    BadAuthenticationMethod = 140,
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
    SharedSubsriptionNotSupported = 158,
    ConnectionRateExceeded = 159,
    MaximumConnectTime = 160,
    SubscriptionIdentifiersNotSupported = 161,
    WildcardSubscriptionsNotSupported = 162,
}
const_enum!(PublishAckReasonCode: u8);

#[derive(Debug, PartialEq, Clone)]
/// Connection Will
pub struct LastWill {
    /// the QoS level to be used when publishing the Will Message.
    pub qos: QoS,
    /// the Will Message is to be Retained when it is published.
    pub retain: bool,
    /// the Will Topic
    pub topic: String<Bytes>,
    /// defines the Application Message that is to be published to the Will Topic
    pub message: Bytes,
}

#[derive(Debug, PartialEq, Clone)]
/// Connect packet content
pub struct Connect {
    /// mqtt protocol version
    pub protocol: Protocol,
    /// the handling of the Session state.
    pub clean_session: bool,
    /// a time interval measured in seconds.
    pub keep_alive: u16,
    /// Will Message be stored on the Server and associated with the Network Connection.
    pub last_will: Option<LastWill>,
    /// identifies the Client to the Server.
    pub client_id: String<Bytes>,
    /// username can be used by the Server for authentication and authorization.
    pub username: Option<String<Bytes>>,
    /// password can be used by the Server for authentication and authorization.
    pub password: Option<Bytes>,
}

type StringPos = (NonZeroU32, u16);

#[derive(Debug, PartialEq, Clone, Copy)]
struct DataRange {
    offset: NonZeroU32,
    len: u16,
}

#[derive(Debug, PartialEq, Clone)]
/// Publish message
pub struct Publish {
    inner: Bytes,
    /// this might be re-delivery of an earlier attempt to send the Packet.
    pub dup: bool,
    pub retain: bool,
    /// the level of assurance for delivery of an Application Message.
    pub qos: QoS,
    pub packet_id: Option<NonZeroU16>,
    topic_place: (NonZeroU16, u16),
    /// only present in PUBLISH Packets where the QoS level is 1 or 2.
    message_expiry_interval: Option<NonZeroU32>,
    topic_alias: Option<NonZeroU16>,
    content_type: Option<DataRange>,
    correlation_data: Option<DataRange>,
    subscription_ids: Vec<u32>,
    response_topic: Option<DataRange>,
    is_text_payload: bool,
    user_properties: Vec<(DataRange, DataRange)>,
    payload_offset: u32,
    // offload: Option<Box<PublishOffload>>
}

impl Publish {
    pub fn topic(&self) -> &str {
        self.map_inner_str(DataRange {
            offset: unsafe { NonZeroU32::new_unchecked(self.topic_place.0.get() as u32) },
            len: self.topic_place.1,
        })
    }

    pub fn topic_string(&self) -> String<Bytes> {
        // string validity was checked on decoding
        unsafe { String::from_utf8_unchecked(self.map_inner_bytes(DataRange {
            offset: unsafe { NonZeroU32::new_unchecked(self.topic_place.0.get() as u32) },
            len: self.topic_place.1,
        }))
        }
    }

    pub fn payload(&self) -> &[u8] {
        &self.inner.as_ref()[self.payload_offset as usize..]
    }

    pub fn response_topic(&self) -> Option<&str> {
        self.response_topic.map(|r| self.map_inner_str(r))
    }

    pub fn content_type(&self) -> Option<&str> {
        self.content_type.map(|r| self.map_inner_str(r))
    }

    pub fn correlation_data(&self) -> Option<&[u8]> {
        self.correlation_data.map(|r| self.map_inner_slice(r))
    }

    pub fn user_properties(&self) -> impl Iterator<Item = (&str, &str)> {
        self.user_properties.iter().map(|(kr, vr)| (self.map_inner_str(*kr), self.map_inner_str(*vr)))
    }

    fn map_inner_bytes(&self, r: DataRange) -> Bytes {
        let offset: usize = r.offset.get() as usize;
        self.inner.slice(offset, offset + (r.len as usize))
    }

    fn map_inner_str(&self, r: DataRange) -> &str {
        std::str::from_utf8_unchecked(self.map_inner_slice(r))
    }

    fn map_inner_slice(&self, r: DataRange) -> &[u8] {
        let offset: usize = r.offset.get() as usize;
        &self.inner.as_ref()[offset..offset + (r.len as usize)]
    }
}

// macro_rules! define_properties {
//     ($name:ident, $pname:ident { $($vn:ident | $vp:ident $(as $multi:tt)?: $vt:ty = $vv:expr),+ }) => {
//         #[derive(Debug, PartialEq, Clone)]
//         pub enum $pname {
//             $($vn($vt)),+
//         }

//         impl $name {
//             $(define_properties!{@prop $($multi)? $pname $vn $vp $vt})+
//         }
//     };
//     (@prop many $pname:ident $vn:ident $vp:ident $vt:ty) => {
//         pub fn $vp(&self) {}
//     };
//     (@prop $pname:ident $vn:ident $vp:ident $vt:ty) => {
//         pub fn $vp(&self) {}
//     };
// }

// define_properties!{
//     Publish, PublishProperty {
//         Utf8Payload | utf8_payload: bool = 0x01,
//         MessageExpiryInterval | message_expiry_interval: u32 = 0x02,
//         ContentType | content_type: String<Bytes> = 0x03,
//         ResponseTopic | response_topic: String<Bytes> = 0x08,
//         CorrelationData | correlation_data: Bytes = 0x9,
//         SubscriptionIdentifier | subscription_identifiers as many: u32 = 0x29,
//         TopicAlias | topic_alias: NonZeroU16 = 0x23,
//         User | user_properties as many: (String<Bytes>, String<Bytes>) = 0x26
//     }
// }

// #[derive(Debug, PartialEq, Clone)]
// pub enum PublishProperty {
//     Utf8Payload(bool),
//     MessageExpiryInterval(u32),
//     ContentType(String<Bytes>),
//     ResponseTopic(String<Bytes>),
//     // CorrelationData(Bytes),// !!
//     SubscriptionIdentifier(u32),
//     // TopicAlias(u16),// !!
//     User(String<Bytes>, String<Bytes>),
// }

#[derive(Debug, PartialEq, Clone)]
pub enum WillProperty {
    Utf8Payload(bool),
    MessageExpiryInterval(u32),
    ContentType(String<Bytes>),
    ResponseTopic(String<Bytes>),
    CorrelationData(Bytes),
    SubscriptionIdentifier(u32),
    WillDelayInterval(u32),
    User(String<Bytes>, String<Bytes>),
}

#[derive(Debug, PartialEq, Clone)]
pub enum ConnectProperty {
    SessionExpiryInterval(u32),
    AuthenticationMethod(String<Bytes>),
    AuthenticationData(Bytes),
    RequestProblemInformation(bool),
    RequestResponseInformation(bool),
    ReceiveMaximum(u16),
    TopicAliasMaximum(u16),
    User(String<Bytes>, String<Bytes>),
    MaximumPacketSize(u32),
}

#[derive(Debug, PartialEq, Clone)]
pub enum ConnAckProperty {
    SessionExpiryInterval(u32),
    AssignedClientIdentifier(String<Bytes>),
    ServerKeepAlive(u16),
    AuthenticationMethod(String<Bytes>),
    AuthenticationData(Bytes),
    ResponseInformation(String<Bytes>),
    ServerReference(String<Bytes>),
    ReasonString(String<Bytes>),
    ReceiveMaximum(u16),
    TopicAliasMaximum(u16),
    MaximumQoS(QoS),
    RetainAvailable(bool),
    User(String<Bytes>, String<Bytes>),
    MaximumPacketSize(u32),
    WildcardSubscriptionAvailable(bool),
    SubscriptionIdentifiersAvailable(bool),
    SharedSubscriptionAvailable(bool),
}

#[derive(Debug, PartialEq, Clone)]
pub enum DisconnectProperty {
    SessionExpiryInterval(u32),
    ServerReference(String<Bytes>),
    ReasonString(String<Bytes>),
    User(String<Bytes>, String<Bytes>),
}

#[derive(Debug, PartialEq, Clone)]
pub enum SubscribeProperty {
    SubscriptionIdentifier(u32),
    User(String<Bytes>, String<Bytes>),
}

#[derive(Debug, PartialEq, Clone)]
pub enum AckProperty {
    ReasonString(String<Bytes>),
    User(String<Bytes>, String<Bytes>),
}

#[derive(Debug, PartialEq, Clone)]
pub enum Property {
    PayloadFormatIndicator(u8),
    MessageExpiryInterval(u32),
    ContentType(String<Bytes>),
    ResponseTopic(String<Bytes>),
    CorrelationData(Bytes),
    SubscriptionIdentifier(u32),
    SessionExpiryInterval(u32),
    AssignedClientIdentifier(String<Bytes>),
    ServerKeepAlive(u16),
    AuthenticationMethod(String<Bytes>),
    AuthenticationData(Bytes),
    RequestProblemInformation(bool),
    WillDelayInterval(u32),
    RequestResponseInformation(bool),
    ResponseInformation(String<Bytes>),
    ServerReference(String<Bytes>),
    ReasonString(String<Bytes>),
    ReceiveMaximum(u16),
    TopicAliasMaximum(u16),
    TopicAlias(u16),
    MaximumQoS(QoS),
    RetainAvailable(bool),
    User(String<Bytes>, String<Bytes>),
    MaximumPacketSize(u32),
    WildcardSubscriptionAvailable(bool),
    SubscriptionIdentifiersAvailable(bool),
    SharedSubscriptionAvailable(bool),
}

#[derive(Debug, PartialEq, Copy, Clone)]
/// Subscribe Return Code
pub enum SubscribeReturnCode {
    Success(QoS),
    Failure,
}

#[derive(Debug, PartialEq, Clone)]
/// MQTT Control Packets
pub enum Packet {
    /// Client request to connect to Server
    Connect(Connect),

    /// Connect acknowledgment
    ConnectAck {
        /// enables a Client to establish whether the Client and Server have a consistent view
        /// about whether there is already stored Session state.
        session_present: bool,
        return_code: ConnectAckReasonCode,
    },

    /// Publish message
    Publish(Publish),

    /// Publish acknowledgment
    PublishAck {
        /// Packet Identifier
        packet_id: u16,
    },
    /// Publish received (assured delivery part 1)
    PublishReceived {
        /// Packet Identifier
        packet_id: u16,
    },
    /// Publish release (assured delivery part 2)
    PublishRelease {
        /// Packet Identifier
        packet_id: u16,
    },
    /// Publish complete (assured delivery part 3)
    PublishComplete {
        /// Packet Identifier
        packet_id: u16,
    },

    /// Client subscribe request
    Subscribe {
        /// Packet Identifier
        packet_id: u16,
        /// the list of Topic Filters and QoS to which the Client wants to subscribe.
        topic_filters: Vec<(String<Bytes>, QoS)>,
    },
    /// Subscribe acknowledgment
    SubscribeAck {
        packet_id: u16,
        /// corresponds to a Topic Filter in the SUBSCRIBE Packet being acknowledged.
        status: Vec<SubscribeReturnCode>,
    },

    /// Unsubscribe request
    Unsubscribe {
        /// Packet Identifier
        packet_id: u16,
        /// the list of Topic Filters that the Client wishes to unsubscribe from.
        topic_filters: Vec<String<Bytes>>,
    },
    /// Unsubscribe acknowledgment
    UnsubscribeAck {
        /// Packet Identifier
        packet_id: u16,
    },

    /// PING request
    PingRequest,
    /// PING response
    PingResponse,

    /// Disconnection is advertised
    Disconnect,

    /// Auth exchange
    Auth,

    /// No response
    Empty,
}

impl Packet {
    #[inline]
    /// MQTT Control Packet type
    pub fn packet_type(&self) -> u8 {
        match *self {
            Packet::Connect { .. } => CONNECT,
            Packet::ConnectAck { .. } => CONNACK,
            Packet::Publish { .. } => PUBLISH,
            Packet::PublishAck { .. } => PUBACK,
            Packet::PublishReceived { .. } => PUBREC,
            Packet::PublishRelease { .. } => PUBREL,
            Packet::PublishComplete { .. } => PUBCOMP,
            Packet::Subscribe { .. } => SUBSCRIBE,
            Packet::SubscribeAck { .. } => SUBACK,
            Packet::Unsubscribe { .. } => UNSUBSCRIBE,
            Packet::UnsubscribeAck { .. } => UNSUBACK,
            Packet::PingRequest => PINGREQ,
            Packet::PingResponse => PINGRESP,
            Packet::Disconnect => DISCONNECT,
            Packet::Auth => AUTH,
            Packet::Empty => DISCONNECT,
        }
    }

    /// Flags specific to each MQTT Control Packet type
    pub fn packet_flags(&self) -> u8 {
        match *self {
            Packet::Publish(Publish {
                dup, qos, retain, ..
            }) => {
                let mut b = qos.into();

                b <<= 1;

                if dup {
                    b |= 0b1000;
                }

                if retain {
                    b |= 0b0001;
                }

                b
            }
            Packet::PublishRelease { .. }
            | Packet::Subscribe { .. }
            | Packet::Unsubscribe { .. } => 0b0010,
            _ => 0,
        }
    }
}

impl From<Connect> for Packet {
    fn from(val: Connect) -> Packet {
        Packet::Connect(val)
    }
}

impl From<Publish> for Packet {
    fn from(val: Publish) -> Packet {
        Packet::Publish(val)
    }
}

pub const CONNECT: u8 = 1;
pub const CONNACK: u8 = 2;
pub const PUBLISH: u8 = 3;
pub const PUBACK: u8 = 4;
pub const PUBREC: u8 = 5;
pub const PUBREL: u8 = 6;
pub const PUBCOMP: u8 = 7;
pub const SUBSCRIBE: u8 = 8;
pub const SUBACK: u8 = 9;
pub const UNSUBSCRIBE: u8 = 10;
pub const UNSUBACK: u8 = 11;
pub const PINGREQ: u8 = 12;
pub const PINGRESP: u8 = 13;
pub const DISCONNECT: u8 = 14;
pub const AUTH: u8 = 15;

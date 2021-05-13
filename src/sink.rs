use std::collections::VecDeque;
use std::fmt;

use actix_ioframe::Sink;
use actix_utils::oneshot;
use bytes::Bytes;
use bytestring::ByteString;
use futures::future::{Future, TryFutureExt};
use mqtt_codec as mqtt;

use crate::cell::Cell;
use std::num::NonZeroU16;
use crate::error::SendPacketError;

#[derive(Clone)]
pub struct MqttSink {
    sink: Sink<mqtt::Packet>,
    pub(crate) inner: Cell<MqttSinkInner>,
}

impl fmt::Debug for MqttSink {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("MqttSink").finish()
    }
}

impl MqttSink {
    pub(crate) fn new(sink: Sink<mqtt::Packet>) -> Self {
        MqttSink {
            sink,
            inner: Cell::new(MqttSinkInner::default()),
        }
    }

    /// Close mqtt connection
    pub fn close(&self) {
        //TODO clear queue
        self.sink.close();
    }

    /// Send publish packet with qos set to 0
    pub fn publish_qos0(&self, topic: ByteString, payload: Bytes, dup: bool) {
        log::trace!("Publish (QoS0) to {:?}", topic);
        let publish = mqtt::Publish {
            topic,
            payload,
            dup,
            retain: false,
            qos: mqtt::QoS::AtMostOnce,
            packet_id: None,
        };
        self.sink.send(mqtt::Packet::Publish(publish));
    }

    pub fn subscribe(
        &mut self,
        topic_filters: Vec<(ByteString, mqtt::QoS)>,
    ) -> SubscribeBuilder {
        SubscribeBuilder {
            id: 0,
            topic_filters,
            inner: self.inner.clone(),
            sink: self.sink.clone(),
        }
    }

    /// Send publish packet
    pub fn publish_qos1(
        &mut self,
        topic: ByteString,
        payload: Bytes,
        dup: bool,
    ) -> impl Future<Output=Result<(), ()>> {
        let (tx, rx) = oneshot::channel();

        let inner = self.inner.get_mut();
        inner.idx += 1;
        if inner.idx == 0 {
            inner.idx = 1
        }
        inner.queue.push_back((inner.idx, tx));

        let publish = mqtt::Publish {
            topic,
            payload,
            dup,
            retain: false,
            qos: mqtt::QoS::AtLeastOnce,
            packet_id: Some(inner.idx),
        };

        self.sink.send(mqtt::Packet::Publish(publish));
        rx.map_err(|_| ())
    }

    pub(crate) fn complete_publish_qos1(&mut self, packet_id: u16) {
        if let Some((idx, tx)) = self.inner.get_mut().queue.pop_front() {
            if idx != packet_id {
                log::trace!(
                    "MQTT protocol error, packet_id order does not match, expected {}, got: {}",
                    idx,
                    packet_id
                );
                self.close();
            } else {
                log::trace!("Ack publish packet with id: {}", packet_id);
                let _ = tx.send(());
            }
        } else {
            log::trace!("Unexpected PublishAck packet");
            self.close();
        }
    }
}

//TODO could make this elsewhere as a shared queue
#[derive(Default)]
pub(crate) struct MqttSinkInner {
    pub(crate) idx: u16,
    pub(crate) queue: VecDeque<(u16, oneshot::Sender<()>)>, //TODO could be map
}

impl MqttSinkInner {
    pub(crate) fn next_id(&mut self) -> u16 {
        // TODO this is clandestine (multiple threads), refcell?
        let idx = self.idx + 1;
        if idx == u16::MAX {
            self.idx = 0;
        } else {
            self.idx = idx;
        }
        self.idx
    }
}

/// Subscribe packet builder
pub struct SubscribeBuilder {
    id: u16,
    // shared: Rc<MqttShared>,
    topic_filters: Vec<(ByteString, mqtt::QoS)>,
    inner: Cell<MqttSinkInner>,
    sink: Sink<mqtt::Packet>,
}

impl SubscribeBuilder {
    /// Set packet id.
    ///
    /// panics if id is 0
    pub fn packet_id(mut self, id: u16) -> Self { // Maybe dont panic here
        if id == 0 {
            panic!("id 0 is not allowed");
        }
        self.id = id;
        self
    }

    /// Add topic filter
    pub fn topic_filter(mut self, filter: ByteString, qos: mqtt::QoS) -> Self {
        self.topic_filters.push((filter, qos));
        self
    }

    #[allow(clippy::await_holding_refcell_ref)]
    /// Send subscribe packet
    pub async fn send(mut self) -> Result<(), SendPacketError> {
        let inner = self.inner.get_mut();
        let filters = self.topic_filters;

        // ack channel
        let (tx, rx) = oneshot::channel();

        // allocate packet id
        let idx = if self.id == 0 { inner.next_id() } else { self.id };
        let contains_duplicate_id = inner.queue.iter()
            .map(|(index, tx)| index)
            .any(|s| *s == idx);
        if contains_duplicate_id {
            return Err(SendPacketError::PacketIdInUse(idx));
        }


        // send subscribe to client
        inner.queue.push_back((idx, tx));
        log::trace!("Sending subscribe packet id: {} filters:{:?}", idx, filters);

        if let Some(id) = NonZeroU16::new(idx) {
            self.sink.send(
                mqtt::Packet::Subscribe {
                    packet_id: u16::from(id),
                    topic_filters: filters,
                }
            );
            rx.await
                .map_err(|e| SendPacketError::Disconnected)
                .map(|_| log::trace!("Subscription of filters {:?} acknowledged", self.topic_filters));

            Ok(())
        } else {
            Err(SendPacketError::PacketId)
        }
    }
}

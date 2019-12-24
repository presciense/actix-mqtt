use std::collections::VecDeque;
use std::fmt;

use actix_ioframe::Sink;
use actix_utils::oneshot;
use bytes::Bytes;
use bytestring::ByteString;
use futures::future::{Future, TryFutureExt};
use mqtt_codec as mqtt;

use crate::cell::Cell;

#[derive(Clone)]
pub struct MqttSink {
    sink: Sink<mqtt::Packet>,
    pub(crate) inner: Cell<MqttSinkInner>,
}

#[derive(Default)]
pub(crate) struct MqttSinkInner {
    pub(crate) idx: u16,
    pub(crate) queue: VecDeque<(u16, oneshot::Sender<()>)>,
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

    /// Send subscribe packet
    pub fn subscribe(
        &mut self,
        topic_filters: Vec<(string::String<Bytes>, mqtt::QoS)>
    ) -> impl Future<Item = (), Error = ()> {
        let (tx, rx) = oneshot::channel();

        let inner = self.inner.get_mut();
        inner.queue.push_back((inner.idx, tx));

        let subscribe = mqtt::Packet::Subscribe {
            topic_filters,
            packet_id: inner.idx,
        };
        self.sink.send(subscribe);
        inner.idx += 1;
        rx.map_err(|_| ())
    }

    /// Send publish packet
    pub fn publish_qos1(
        &mut self,
        topic: ByteString,
        payload: Bytes,
        dup: bool,
    ) -> impl Future<Item = (), Error = ()> {
        log::trace!("Publish (QoS1) to {:?}", topic);
        let (tx, rx) = oneshot::channel();

        let inner = self.inner.get_mut();
        inner.idx += 1;
        if inner.idx == 0 {
            inner.idx = 1
        }
        inner.queue.push_back((inner.idx, tx));

        let publish = mqtt::Packet::Publish(mqtt::Publish {
            topic,
            payload,
            dup,
            retain: false,
            qos: mqtt::QoS::AtLeastOnce,
            packet_id: Some(inner.idx),
<<<<<<< variant A
        });
        log::trace!("Publish (QoS1) to {:#?}", publish);

        self.sink.send(publish);
>>>>>>> variant B
        };
        self.sink.send(mqtt::Packet::Publish(publish));
======= end
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

impl fmt::Debug for MqttSink {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("MqttSink").finish()
    }
}

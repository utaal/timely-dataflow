//! Traits, implementations, and macros related to logging timely events.


use std::cell::RefCell;
use std::io::Write;
use std::fs::File;
use std::rc::Rc;
use std::sync::{Arc, RwLock};

use ::Data;

use timely_communication::Allocate;
use timely_communication;
use ::progress::timestamp::RootTimestamp;
use ::progress::nested::product::Product;
use ::progress::timestamp::Timestamp;

use byteorder::{LittleEndian, WriteBytesExt};

use dataflow::scopes::root::Root;
use dataflow::operators::capture::{EventWriter, Event, EventPusher};

use abomonation::Abomonation;

use std::io::BufWriter;
use std::net::TcpStream;

use timely_logging;
use timely_logging::BufferingLogger;
use timely_logging::Event as LogEvent;
use timely_logging::EventsSetup;
use timely_logging::{CommsEvent, CommsSetup};

/// TODO(andreal)
pub type Logger = Rc<BufferingLogger<LogEvent>>;

/// TODO(andreal)
pub struct LogManager {
    /// TODO(andreal)
    pub timely_logging: Arc<Fn(EventsSetup)->Rc<BufferingLogger<LogEvent>>+Send+Sync>,
    /// TODO(andreal)
    pub communication_logging: Arc<Fn(CommsSetup)->Rc<BufferingLogger<CommsEvent>>+Send+Sync>,
}

impl Default for LogManager {
    fn default() -> Self {
        LogManager {
            timely_logging: Arc::new(|_| Rc::new(BufferingLogger::new())),
            communication_logging: Arc::new(|_| Rc::new(BufferingLogger::new())),
        }
    }
}

trait EventStreamInput<T: Timestamp, V: Clone> {
    fn send(&mut self, value: V);
    fn size(&self) -> usize;
    fn flush(&mut self);
    fn advance_by(&mut self, timestamp: T);
    fn clear(&mut self);
}

/// Logs events to an underlying writer.
pub struct EventStreamWriter<T: Timestamp, V: Clone, P: EventPusher<T, V>> {
    buffer: Vec<V>,
    pusher: P,
    cur_time: T,
    _v: ::std::marker::PhantomData<V>,
}

impl<T: Timestamp, V: Clone, P: EventPusher<T, V>> EventStreamWriter<T, V, P> {
    fn new(mut event_pusher: P) -> EventStreamWriter<T, V, P> {
        eprintln!("created");
        let cur_time: T = Default::default();
        EventStreamWriter {
            buffer: Vec::new(),
            pusher: event_pusher,
            cur_time: cur_time,
            _v: ::std::marker::PhantomData,
        }
    }
}

impl<T: Timestamp, V: Clone, P: EventPusher<T, V>> EventStreamInput<T, V> for EventStreamWriter<T, V, P> {
    fn send(&mut self, value: V) {
        self.buffer.push(value);
    }
    fn size(&self) -> usize {
        self.buffer.len()
    }
    fn flush(&mut self) {
        if self.buffer.len() > 0 {
            self.pusher.push(Event::Messages(self.cur_time.clone(), self.buffer.clone()));
        }
        self.buffer.clear();
    }
    fn advance_by(&mut self, timestamp: T) {
        assert!(self.cur_time.less_equal(&timestamp));
        self.flush();
        self.pusher.push(Event::Progress(vec![(self.cur_time.clone(), -1), (timestamp.clone(), 1)]));
        self.cur_time = timestamp;
    }
    fn clear(&mut self) {
        self.buffer.clear();
    }
}

impl<T: Timestamp, V: Clone, P: EventPusher<T, V>> Drop for EventStreamWriter<T, V, P> {
    fn drop(&mut self) {
        if self.buffer.len() > 0 {
            self.pusher.push(Event::Messages(self.cur_time.clone(), self.buffer.clone()));
            self.buffer.clear();
        }
        self.pusher.push(Event::Progress(vec![(self.cur_time.clone(), -1)]));
        eprintln!("dropped: {:?}", self.cur_time);
    }
}

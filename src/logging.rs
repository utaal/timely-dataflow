//! Traits, implementations, and macros related to logging timely events.


use std::cell::RefCell;
use std::io::Write;
use std::fs::File;
use std::rc::Rc;
use std::sync::{Arc, RwLock, Mutex};
use std::hash::Hash;

use std::collections::HashMap;

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
    timely_logs: Arc<Mutex<HashMap<
        EventsSetup,
        Arc<Mutex<EventStreamSubscriptionManager<LogEvent>>>>>>,
    communication_logs: Arc<Mutex<HashMap<
        CommsSetup,
        Arc<Mutex<EventStreamSubscriptionManager<CommsEvent>>>>>>,
}

/// TODO(andreal)
pub struct LoggerConfig {
    /// TODO(andreal)
    pub timely_logging: Arc<Fn(EventsSetup)->Rc<BufferingLogger<LogEvent>>+Send+Sync>,
    /// TODO(andreal)
    pub communication_logging: Arc<Fn(CommsSetup)->Rc<BufferingLogger<CommsEvent>>+Send+Sync>,
}

impl LoggerConfig {
    fn make_logger<S: Eq+Hash, E: Clone+'static>(
        setup: S,
        logs: Arc<Mutex<HashMap<S, Arc<Mutex<EventStreamSubscriptionManager<E>>>>>>)
        -> Rc<BufferingLogger<E>> {

        let event_manager: Arc<Mutex<EventStreamSubscriptionManager<E>>> =
            Arc::new(Mutex::new(Default::default()));

        logs.lock().unwrap().insert(
            setup,
            event_manager.clone());

        Rc::new(BufferingLogger::new(Box::new(move |evs| {
            for pusher in event_manager.lock().unwrap().event_pushers.iter_mut() {
                pusher.push(Event::Messages(event_manager.lock().unwrap().frontier, evs.clone()));
            }
        })))
    }

    /// TODO(andreal)
    pub fn new(log_manager: &mut LogManager) -> Self {
        let timely_logs = log_manager.timely_logs.clone();
        let communication_logs = log_manager.communication_logs.clone();
        LoggerConfig {
            timely_logging: Arc::new(move |events_setup: EventsSetup| {
                LoggerConfig::make_logger(events_setup, timely_logs.clone())
            }),
            communication_logging: Arc::new(move |comms_setup: CommsSetup| {
                LoggerConfig::make_logger(comms_setup, communication_logs.clone())
            }),
        }
    }
}

impl Default for LoggerConfig {
    fn default() -> Self {
        LoggerConfig {
            timely_logging: Arc::new(|_| Rc::new(BufferingLogger::new(Box::new(|_| {})))),
            communication_logging: Arc::new(|_| Rc::new(BufferingLogger::new(Box::new(|_| {})))),
        }
    }
}

struct EventStreamSubscriptionManager<E> {
    frontier: Product<RootTimestamp, u64>,
    event_pushers: Vec<Box<EventPusher<Product<RootTimestamp, u64>, E>+Send+Sync>>,
}

impl<E> Default for EventStreamSubscriptionManager<E> {
    fn default() -> Self {
        EventStreamSubscriptionManager {
            frontier: Default::default(),
            event_pushers: Vec::new(),
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

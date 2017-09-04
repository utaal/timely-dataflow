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

use timely_logging::BufferingLogger;
use timely_logging::Event as LogEvent;
use timely_logging::EventsSetup;
use timely_logging::{CommsEvent, CommsSetup};
use timely_logging::LoggerBatch;

/// TODO(andreal)
pub type Logger = Rc<BufferingLogger<LogEvent>>;

/// TODO(andreal)
pub struct LogManager {
    timely_logs: HashMap<
        EventsSetup,
        Arc<Mutex<EventStreamSubscriptionManager<LogEvent>>>>,
    timely_subscriptions:
        Vec<(Box<Fn(&EventsSetup)->bool+Send+Sync>, Box<EventPusher<Product<RootTimestamp, u64>, (u64, LogEvent)>+Send+Sync>)>,
    communication_logs: HashMap<
        CommsSetup,
        Arc<Mutex<EventStreamSubscriptionManager<CommsEvent>>>>,
    communication_subscriptions:
        Vec<(Box<Fn(&EventsSetup)->bool+Send+Sync>, Box<EventPusher<Product<RootTimestamp, u64>, (u64, CommsEvent)>+Send+Sync>)>,
}

/// TODO(andreal)
pub struct FilteredLogManager<S, E> {
    log_manager: Arc<Mutex<LogManager>>,
    filter: Box<Fn(&S)->bool>,
    _e: ::std::marker::PhantomData<E>,
}

impl LogManager {
    /// TODO(andreal)
    pub fn new() -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(LogManager {
            timely_logs: HashMap::new(),
            timely_subscriptions: Vec::new(),
            communication_logs: HashMap::new(),
            communication_subscriptions: Vec::new(),
        }))
    }
}

/// TODO(andreal)
pub trait LogFilter {
    /// TODO(andreal)
    fn workers(&mut self) -> FilteredLogManager<EventsSetup, LogEvent>;
}

impl LogFilter for Arc<Mutex<LogManager>> {
    /// TODO(andreal)
    #[inline] fn workers(&mut self) -> FilteredLogManager<EventsSetup, LogEvent> {
        FilteredLogManager {
            log_manager: self.clone(),
            filter: Box::new(|_| true),
            _e: ::std::marker::PhantomData,
        }
    }
}

/// TODO(andreal)
pub struct LoggerConfig {
    /// TODO(andreal)
    pub timely_logging: Arc<Fn(EventsSetup)->Rc<BufferingLogger<LogEvent>>+Send+Sync>,
    /// TODO(andreal)
    pub communication_logging: Arc<Fn(CommsSetup)->Rc<BufferingLogger<CommsEvent>>+Send+Sync>,
}

impl LoggerConfig {
    fn register_timely_logger(
        log_manager: &mut LogManager,
        events_setup: EventsSetup) -> Arc<Mutex<EventStreamSubscriptionManager<LogEvent>>> {

        let event_manager: Arc<Mutex<EventStreamSubscriptionManager<LogEvent>>> = Arc::new(Mutex::new(Default::default()));
        log_manager.timely_logs.insert(events_setup, event_manager.clone());
        // TODO(andreal) update subscriptions
        event_manager
    }

    fn register_comms_logger(
        log_manager: &mut LogManager,
        comms_setup: CommsSetup) -> Arc<Mutex<EventStreamSubscriptionManager<CommsEvent>>> {

        let event_manager: Arc<Mutex<EventStreamSubscriptionManager<CommsEvent>>> = Arc::new(Mutex::new(Default::default()));
        log_manager.communication_logs.insert(comms_setup, event_manager.clone());
        // TODO(andreal) update subscriptions
        event_manager
    }

    /// TODO(andreal)
    pub fn new(log_manager: &mut Arc<Mutex<LogManager>>) -> Self {
        let timely_logging_manager = log_manager.clone();
        let communication_logging_manager = log_manager.clone();
        LoggerConfig {
            timely_logging: Arc::new(move |events_setup: EventsSetup| {
                let subscription_manager = LoggerConfig::register_timely_logger(
                    &mut timely_logging_manager.lock().unwrap(), events_setup);
                Rc::new(BufferingLogger::new(Box::new(move |data| {
                    subscription_manager.lock().unwrap().publish_batch(data);
                })))
            }),
            communication_logging: Arc::new(move |comms_setup: CommsSetup| {
                let subscription_manager = LoggerConfig::register_comms_logger(
                    &mut communication_logging_manager.lock().unwrap(), comms_setup);
                Rc::new(BufferingLogger::new(Box::new(move |data| {
                    subscription_manager.lock().unwrap().publish_batch(data);
                })))
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
    event_pushers: Vec<Box<EventPusher<Product<RootTimestamp, u64>, (u64, E)>+Send+Sync>>,
}

impl<E> Default for EventStreamSubscriptionManager<E> {
    fn default() -> Self {
        EventStreamSubscriptionManager {
            frontier: Default::default(),
            event_pushers: Vec::new(),
        }
    }
}

impl<E: Clone> EventStreamSubscriptionManager<E> {
    pub fn publish_batch(&mut self, logger_batch: LoggerBatch<E>) -> () {
        for pusher in self.event_pushers.iter_mut() {
            match logger_batch {
                LoggerBatch::Logs(evs) => {
                    pusher.push(Event::Messages(self.frontier, evs.clone()));
                    let &(last_ts, _) = evs.last().unwrap();
                    let new_frontier = RootTimestamp::new(last_ts);
                    pusher.push(Event::Progress(vec![(new_frontier, 1), (self.frontier, -1)]));
                    self.frontier = new_frontier;
                },
                LoggerBatch::End => {
                    pusher.push(Event::Progress(vec![(self.frontier, -1)]));
                },
            }
        }
    }
}

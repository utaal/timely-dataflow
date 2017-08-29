use std::cell::RefCell;
use std::fmt::Debug;
use std::net::TcpStream;
use std::rc::Rc;

use abomonation::Abomonation;

use timely_logging::{Logger, CommEvent, CommsSetup};
pub use timely_logging::CommunicationEvent;
pub use timely_logging::SerializationEvent;

pub trait LogSender {
    fn advance_by(&self, timestamp: u64);
    fn send(&self, setup: CommsSetup, event: CommEvent);
}

pub fn initialize(process: usize, sender: bool, index: usize,
                  logging: Rc<LogSender>) {
    let setup = CommsSetup {
        process: process,
        sender: sender,
        remote: index,
    };
}


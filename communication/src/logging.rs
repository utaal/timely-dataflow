use std::cell::RefCell;
use std::fmt::Debug;
use std::net::TcpStream;
use std::rc::Rc;

use abomonation::Abomonation;

use timely_logging::{Logger, CommEvent, CommsSetup};
pub use timely_logging::CommunicationEvent;
pub use timely_logging::SerializationEvent;

pub type LogSender = Rc<Fn(CommEvent)->()>;


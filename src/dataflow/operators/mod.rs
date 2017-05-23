//! Extension traits for `Stream` implementing various operators.
//!
//! A collection of functions taking typed `Stream` objects as input and producing new `Stream`
//! objects as output. Many of the operators provide simple, composable functionality. Some of the
//! operators are more complicated, for use with advanced timely dataflow features.
//!
//! The [`Unary`](./unary/index.html) and [`Binary`](./binary/index.html) operators provide general
//! operators whose behavior can be supplied using closures accepting input and output handles.
//! Most of the operators in this module are defined using these two general operators.

pub use self::enterleave::{Enter, EnterAt, Leave};
pub use self::unary::Unary;
// pub use self::queue::*;
pub use self::input::Input;
pub use self::unordered_input::UnorderedInput;
pub use self::feedback::{LoopVariable, ConnectLoop};
pub use self::concat::{Concat, Concatenate};
pub use self::partition::Partition;
pub use self::map::Map;
pub use self::inspect::Inspect;
pub use self::filter::Filter;
pub use self::binary::Binary;
pub use self::delay::Delay;
pub use self::exchange::Exchange as ExchangeExtension;
pub use self::broadcast::Broadcast;
pub use self::probe::Probe;
pub use self::to_stream::ToStream;
pub use self::capture::Capture;
pub use self::operator::Operator;
pub use self::to_timely_stream::ToTimelyStream;

pub use self::reclock::Reclock;
pub use self::count::Accumulate;

pub mod enterleave;
pub mod unary;
// pub mod queue;
pub mod input;
pub mod unordered_input;
pub mod feedback;
pub mod concat;
pub mod partition;
pub mod map;
pub mod inspect;
pub mod filter;
pub mod binary;
pub mod delay;
pub mod exchange;
pub mod broadcast;
pub mod probe;
pub mod to_stream;
pub mod capture;
pub mod operator;
pub mod to_timely_stream;

pub mod aggregation;

pub mod reclock;
pub mod count;

// keep the handle constructors private
mod handles;
pub use self::handles::{InputHandle, FrontieredInputHandle, OutputHandle};

mod notificator;
pub use self::notificator::Notificator;
pub use self::notificator::FrontierNotificator;

// keep "mint" module-private
mod capability;
pub use self::capability::{Capability, CapabilitySet};

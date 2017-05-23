//! Create new `Streams` connected to the receiving end of an mpsc channel.

use std::sync::mpsc::{Receiver};
use dataflow::operators::{OutputHandle};

use progress::{Activity, Timestamp};

use Data;
use dataflow::channels::pushers::{Tee};

use dataflow::operators::operator::source;
use dataflow::operators::Capability;

use dataflow::{Stream, Scope};

/// Represents the input of a to_timely_stream operator.
pub enum StreamInput<'a, X: 'a> {
    /// A slice of data was received.
    Data(&'a [X]),
    /// The channel was closed.
    Closed,
}

/// Converts to a timely `Stream`.
pub trait ToTimelyStream<X: 'static, T: Timestamp> {
    /// Converts to a timely `Stream` using the logic defined by `builder`.
    fn to_timely_stream<S: Scope<Timestamp=T>, B, L, D: Data>(self, scope: &mut S, builder: B) -> Stream<S, D>
        where
            B: FnOnce(Capability<S::Timestamp>) -> L,
            L: FnMut(StreamInput<X>, &mut OutputHandle<S::Timestamp, D, Tee<S::Timestamp, D>>)->()+'static;
}


impl<X: 'static, T: Timestamp> ToTimelyStream<X, T> for Receiver<X> {
    fn to_timely_stream<S: Scope<Timestamp=T>, B, L, D: Data>(self, scope: &mut S, builder: B) -> Stream<S, D>
        where
            B: FnOnce(Capability<S::Timestamp>) -> L,
            L: FnMut(StreamInput<X>, &mut OutputHandle<S::Timestamp, D, Tee<S::Timestamp, D>>)->()+'static {
        source(scope, "ToTimelyStream", move |capability| {
            let mut logic = (builder)(capability);
            let receiver = self;
            let mut data = Vec::with_capacity(1024);
            let mut disconnected = false;
            move |output| {
                if !disconnected {
                    data.clear();
                    loop {
                        match receiver.try_recv() {
                            Ok(d) => {
                                data.push(d);
                            },
                            Err(::std::sync::mpsc::TryRecvError::Empty) => break,
                            Err(::std::sync::mpsc::TryRecvError::Disconnected) => {
                                disconnected = true;
                                break;
                            }
                        }
                    }
                    logic(StreamInput::Data(&data[..]), output);
                    if disconnected {
                        logic(StreamInput::Closed, output);
                    }
                }
                Activity::Done
            }
        })
    }
}

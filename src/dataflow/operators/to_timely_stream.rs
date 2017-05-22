//! Create new `Streams` connected to the receiving end of an mpsc channel.

use std::sync::mpsc::{Receiver};
use dataflow::operators::{OutputHandle};

use progress::{Activity, Timestamp};

use Data;
use dataflow::channels::pushers::{Tee};

use dataflow::operators::operator::source;
use dataflow::operators::Capability;

use dataflow::{Stream, Scope};

/// TODO
pub enum StreamInput<'a, X: 'a> {
    /// TODO
    Data(&'a [X]),
    /// TODO
    Closed,
}

/// TODO
pub trait ToTimelyStream<X: 'static, T: Timestamp> {
    /// TODO
    fn to_timely_stream<S: Scope<Timestamp=T>, B, L, D: Data>(self, scope: &mut S, builder: B) -> Stream<S, D>
        where
            B: FnOnce(Capability<S::Timestamp>) -> L,
            L: FnMut(StreamInput<X>, &mut OutputHandle<S::Timestamp, D, Tee<S::Timestamp, D>>)->()+'static;
}


impl<X: 'static, T: Timestamp> ToTimelyStream<X, T> for Receiver<X> {
    fn to_timely_stream<S: Scope<Timestamp=T>, B, L, D: Data>(self, scope: &mut S, mut builder: B) -> Stream<S, D>
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

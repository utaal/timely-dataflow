//! Traits and types describing timely dataflow events.
//! 
//! The `Event` type describes the information an operator can observe about a timely dataflow
//! stream. There are two types of events, (i) the receipt of data and (ii) reports of progress
//! of timestamps.

use abomonation::Abomonation;

/// Data and progress events of the captured stream.
#[derive(Debug)]
pub enum Event<T, D> {
    /// An initial marker, used only to start the linked list implementation.
    Start,
    /// Progress received via `push_external_progress`.
    Progress(Vec<(T, i64)>),
    /// Messages received via the data stream.
    Messages(T, Vec<D>),
}

impl<T: Abomonation, D: Abomonation> Abomonation for Event<T,D> {
    #[inline] unsafe fn embalm(&mut self) {
        if let Event::Progress(ref mut vec) = *self { vec.embalm(); }
        if let Event::Messages(ref mut time, ref mut data) = *self { time.embalm(); data.embalm(); }
    }
    #[inline] unsafe fn entomb(&self, bytes: &mut Vec<u8>) {
        if let Event::Progress(ref vec) = *self { vec.entomb(bytes); }
        if let Event::Messages(ref time, ref data) = *self { time.entomb(bytes); data.entomb(bytes); }
    }
    #[inline] unsafe fn exhume<'a, 'b>(&'a mut self, mut bytes: &'b mut[u8]) -> Option<&'b mut [u8]> {
        match *self {
            Event::Progress(ref mut vec) => { vec.exhume(bytes) },
            Event::Messages(ref mut time, ref mut data) => {
                let temp = bytes; bytes = if let Some(bytes) = time.exhume(temp) { bytes } else { return None; };
                let temp = bytes; bytes = if let Some(bytes) = data.exhume(temp) { bytes } else { return None; };
                Some(bytes)
            }
            Event::Start => Some(bytes),
        }
    }
}

/// Iterates over contained `Event<T, D>`.
///
/// The `EventIterator` trait describes types that can iterate over references to events,
/// and which can be used to replay a stream into a new timely dataflow computation.
pub trait EventIterator<T, D> {
    /// Iterates over references to `Event<T, D>` elements.
    fn next(&mut self) -> Option<&Event<T, D>>;
}


/// Receives `Event<T, D>` events.
pub trait EventPusher<T, D> {
    /// Provides a new `Event<T, D>` to the pusher.
    fn push(&self, event: Event<T, D>);
}


// implementation for the linked list behind a `Handle`.
impl<T, D> EventPusher<T, D> for ::std::sync::mpsc::Sender<Event<T, D>> {
    fn push(&self, event: Event<T, D>) {
        self.send(event).unwrap();
    }
}

/// TODO(andreal)
pub struct BlackholeEventPusher { }

impl<T, D> EventPusher<T, D> for BlackholeEventPusher {
    fn push(&self, event: Event<T, D>) {
        // no-op
    }
}

/// A linked-list event pusher and iterator.
pub mod link {

    use std::rc::Rc;
    use std::cell::RefCell;

    use super::{Event, EventPusher, EventIterator};

    /// A linked list of Event<T, D>.
    pub struct EventLink<T, D> {
        /// An event.
        pub event: Event<T, D>,
        /// The next event, if it exists.
        pub next: RefCell<Option<Rc<EventLink<T, D>>>>,
    }

    impl<T, D> EventLink<T, D> { 
        /// Allocates a new `EventLink`.
        /// 
        /// This could be improved with the knowledge that the first event should always be a frontier(default).
        /// We could, in principle, remove the `Event::Start` event, and thereby no longer have to explain it.
        pub fn new() -> EventLink<T, D> { 
            EventLink { event: Event::Start, next: RefCell::new(None) }
        }
    }

    // implementation for the linked list behind a `Handle`.
    impl<T, D> EventPusher<T, D> for RefCell<Rc<EventLink<T, D>>> {
        fn push(&self, event: Event<T, D>) {
            let mut this = self.borrow_mut();
            *this.next.borrow_mut() = Some(Rc::new(EventLink { event: event, next: RefCell::new(None) }));
            let next = this.next.borrow().as_ref().unwrap().clone();
            *this = next;
        }
    }

    impl<T, D> EventIterator<T, D> for Rc<EventLink<T, D>> {
        fn next(&mut self) -> Option<&Event<T, D>> {
            let is_some = self.next.borrow().is_some();
            if is_some {
                let next = self.next.borrow().as_ref().unwrap().clone();
                *self = next;
                Some(&self.event)
            }
            else {
                None
            }
        }
    }
}

/// A binary event pusher and iterator.
pub mod binary {

    use std::cell::RefCell;
    use std::io::Write;
    use abomonation::Abomonation;
    use super::{Event, EventPusher, EventIterator};

    /// A wrapper for `W: Write` implementing `EventPusher<T, D>`.
    pub struct EventWriter<T, D, W: ::std::io::Write> {
        buffer: RefCell<Vec<u8>>,
        stream: RefCell<W>,
        phant: ::std::marker::PhantomData<(T,D)>,
    }

    impl<T, D, W: ::std::io::Write> EventWriter<T, D, W> {
        /// Allocates a new `EventWriter` wrapping a supplied writer.
        pub fn new(w: W) -> EventWriter<T, D, W> {
            EventWriter {
                buffer: RefCell::new(vec![]),
                stream: RefCell::new(w),
                phant: ::std::marker::PhantomData,
            }
        }
    }

    impl<T: Abomonation, D: Abomonation, W: ::std::io::Write> EventPusher<T, D> for EventWriter<T, D, W> {
        fn push(&self, event: Event<T, D>) {
            unsafe { ::abomonation::encode(&event, &mut self.buffer.borrow_mut()); }
            self.stream.borrow_mut().write_all(&self.buffer.borrow()[..]).unwrap();
            self.buffer.borrow_mut().clear();
        }
    }

    /// A Wrapper for `R: Read` implementing `EventIterator<T, D>`.
    pub struct EventReader<T, D, R: ::std::io::Read> {
        reader: R,
        bytes: Vec<u8>,
        buff1: Vec<u8>,
        buff2: Vec<u8>,
        consumed: usize,
        valid: usize,
        phant: ::std::marker::PhantomData<(T,D)>,
    }

    impl<T, D, R: ::std::io::Read> EventReader<T, D, R> {
        /// Allocates a new `EventReader` wrapping a supplied reader.
        pub fn new(r: R) -> EventReader<T, D, R> {
            EventReader {
                reader: r,
                bytes: vec![0u8; 1 << 20],
                buff1: vec![],
                buff2: vec![],
                consumed: 0,
                valid: 0,
                phant: ::std::marker::PhantomData,
            }
        }
    }

    impl<T: Abomonation, D: Abomonation, R: ::std::io::Read> EventIterator<T, D> for EventReader<T, D, R> {
        fn next(&mut self) -> Option<&Event<T, D>> {

            // if we can decode something, we should just return it! :D
            if unsafe { ::abomonation::decode::<Event<T,D>>(&mut self.buff1[self.consumed..]) }.is_some() {
                let (item, rest) = unsafe { ::abomonation::decode::<Event<T,D>>(&mut self.buff1[self.consumed..]) }.unwrap();
                self.consumed = self.valid - rest.len();
                return Some(item);
            }
            // if we exhaust data we should shift back (if any shifting to do)
            if self.consumed > 0 {
                self.buff2.clear();
                self.buff2.write_all(&self.buff1[self.consumed..]).unwrap();
                ::std::mem::swap(&mut self.buff1, &mut self.buff2);
                self.valid = self.buff1.len();
                self.consumed = 0;
            }

            if let Ok(len) = self.reader.read(&mut self.bytes[..]) {
                self.buff1.write_all(&self.bytes[..len]).unwrap();
                self.valid = self.buff1.len();
            }

            None
        }
    }
}

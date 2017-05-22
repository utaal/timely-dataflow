//! TODO doc

#[cfg(feature = "sleeping")]
use std::sync::{Arc, Mutex, Condvar};
use std::sync::mpsc::{Sender, Receiver, SendError, channel};
use std::rc::Rc;

use NotifyAll;

/// TODO
pub struct Notifier<NA: NotifyAll> {
    notify_all: Arc<NA>,
}

impl<NA: NotifyAll> Notifier<NA> {
    /// TODO
    pub fn new(notify_all: Arc<NA>) -> Notifier<NA> {
        Notifier {
            notify_all: notify_all,
        }
    }
}

impl<NA: NotifyAll> Clone for Notifier<NA> {
    fn clone(&self) -> Self {
        Notifier {
            notify_all: self.notify_all.clone(),
        }
    }
}

impl<NA: NotifyAll> Notifier<NA> {
    /// TODO
    pub fn new_async_channel<T: Send>(&self) -> (NotifyingSender<NA, T>, Receiver<T>) {
        let (sender, receiver) = channel();
        (
            NotifyingSender {
                notifier: self.clone(),
                sender: Some(sender),
            },
            receiver
        )
    }
}

/// TODO
pub struct NotifyingSender<NA: NotifyAll, T: Send> {
    notifier: Notifier<NA>,
    sender: Option<Sender<T>>,
}

impl<NA: NotifyAll, T: Send> NotifyingSender<NA, T> {
    /// TODO
    pub fn send(&self, t: T) -> Result<(), SendError<T>> {
        let result = self.sender.as_ref().expect("sender should be some").send(t);
        if result.is_ok() {
            self.notifier.notify_all.notify_all();
        }
        result
    }
}

impl<NA: NotifyAll, T: Send> Drop for NotifyingSender<NA, T> {
    fn drop(&mut self) {
        self.sender = None;
        self.notifier.notify_all.notify_all();
        ::std::thread::sleep(::std::time::Duration::from_millis(100));
    }
}

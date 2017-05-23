#![cfg(feature = "sleeping")]
//! Support for the sleeping feature. APIs to run a computation that wait()s for input instead of
//! spinning indefinitely. Provides channels that allow input threads to wake up the workers.

use time;

use std::sync::{Arc, Mutex, Condvar};
use std::sync::mpsc::{Sender, Receiver, SendError, channel};

use NotifyAll;

use timely_communication::{initialize, initialize_sleep, Configuration, Allocator, WorkerGuards};
use timely_communication::NotifyHook;
use dataflow::scopes::Root;
use progress::Activity;

/// Executes a timely computation.
pub trait Runner<NA: NotifyAll> {
    /// Executes a timely dataflow from supplied arguments and per-communicator logic.
    ///
    /// Refer to [execute_from_args](../../timely/execute/fn.execute_from_args.html) for details.
    fn execute_from_args<I, T, F>(iter: I, func: F) -> Result<WorkerGuards<T>,String> 
        where I: Iterator<Item=String>, 
              T:Send+'static,
              F: Fn(&mut Root<Allocator>, Notifier<NA>)->T+Send+Sync+'static;

    /// Executes a timely dataflow from a configuration and per-communicator logic.
    ///
    /// Refer to [execute](../../timely/execute/fn.execute.html) for details.
    fn execute<T, F>(config: Configuration, func: F) -> Result<WorkerGuards<T>, String>
where T:Send+'static,
      F: Fn(&mut Root<Allocator>, Notifier<NA>)->T+Send+Sync+'static;
}

/// Marker struct for the timely_communication-based implementation of Runner.
pub struct Communication;

const SPIN_TIME_NS: u64 = 80_000_000;

impl Runner<NotifyHook> for Communication {
    fn execute_from_args<I, T, F>(iter: I, func: F) -> Result<WorkerGuards<T>,String> 
        where I: Iterator<Item=String>, 
              T:Send+'static,
              F: Fn(&mut Root<Allocator>, Notifier<NotifyHook>)->T+Send+Sync+'static, {
        Self::execute(try!(Configuration::from_args(iter)), func)
    }

    fn execute<T, F>(config: Configuration, func: F) -> Result<WorkerGuards<T>, String>
    where T:Send+'static,
          F: Fn(&mut Root<Allocator>, Notifier<NotifyHook>)->T+Send+Sync+'static {

        let notify_hook = Arc::new(NotifyHook::new());
        let mutex = Arc::new(Mutex::new(0));
        let condvar = Arc::new(Condvar::new());

        {
            let mutex = mutex.clone();
            let condvar = condvar.clone();
            notify_hook.subscribe(Box::new(move || {
                {
                    let mut counter = mutex.lock().unwrap();
                    *counter += 1;
                }
                condvar.notify_all();
            }));
        }

        initialize_sleep(config, notify_hook.clone(), move |allocator| {
            #[cfg(feature = "verbose")]
            let index = allocator.index();
            let mut root = Root::new(allocator);
            let result = func(&mut root, Notifier::new(notify_hook.clone()));
            let mut inactive_since = None;
            let mut client_counter = 0;
            #[cfg(feature = "verbose")]
            let mut went_asleep = 0;
            #[cfg(feature = "verbose")]
            let mut step_count = 0;
            while {
                #[cfg(feature = "verbose")]
                {
                    step_count += 1;
                    if step_count % 1000000 == 0 {
                        println!("[{}] steps: {}", index, step_count);
                    }
                }
                let (active, activity) = root.step();
                if activity == Activity::Done {
                    if inactive_since.is_none() {
                        inactive_since = Some(time::precise_time_ns());
                    }
                    if time::precise_time_ns() - inactive_since.unwrap_or(time::precise_time_ns()) > SPIN_TIME_NS {
                        #[cfg(feature = "verbose")]
                        {
                            went_asleep += 1;
                            if went_asleep % 100000 == 0 {
                                println!("[{}] slept: {}", index, went_asleep);
                            }
                        }
                        //sleep_wake.wait(&mut client_counter);
                        let mut counter = mutex.lock().unwrap();
                        if *counter == client_counter {
                            counter = condvar.wait(counter).unwrap();
                        }
                        client_counter = *counter;
                        inactive_since = None;
                    }
                } else {
                    inactive_since = None;
                }
                active
            } { }
            result
        })
      }
}

/// Provides an interface to wake up worker threads.
pub struct Notifier<NA: NotifyAll> {
    notify_all: Arc<NA>,
}

impl<NA: NotifyAll> Notifier<NA> {
    /// Construct a Notifier.
    fn new(notify_all: Arc<NA>) -> Notifier<NA> {
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
    /// Construct a new asynchronous channel that wraps an mpsc channel with logic to wake up the
    /// worker threads.
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

/// Wrapper of an mpsc Sender side that wakes up the worker threads whenever a piece of data is
/// sent.
pub struct NotifyingSender<NA: NotifyAll, T: Send> {
    notifier: Notifier<NA>,
    sender: Option<Sender<T>>,
}

impl<NA: NotifyAll, T: Send> NotifyingSender<NA, T> {
    /// Sends `t` to the channel and notifies worker threads.
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

impl<NA: NotifyAll, T: Send> Clone for NotifyingSender<NA, T> {
    fn clone(&self) -> Self {
        NotifyingSender {
            notifier: self.notifier.clone(),
            sender: self.sender.clone(),
        }
    }
}

use progress::frontier::MutableAntichain;
use progress::Timestamp;
use progress::count_map::CountMap;
use dataflow::operators::Capability;

/// Tracks requests for notification and delivers available notifications.
///
/// Notificator is meant to manage the delivery of requested notifications in the presence of
/// inputs that may have outstanding messages to deliver. The notificator tracks the frontiers,
/// as presented from the outside, for each input. Requested notifications can be served only
/// once there are no frontier elements less-or-equal to them, and there are no other pending
/// notification requests less than them. Each with be less-or-equal to itself, so we want to
/// dodge that corner case.
pub struct Notificator<T: Timestamp> {
    pending: Vec<Capability<T>>,
    frontier: Vec<MutableAntichain<T>>,
}

impl<T: Timestamp> Notificator<T> {
    pub fn new() -> Notificator<T> {
        Notificator {
            pending: Vec::new(),
            frontier: Vec::new(),
        }
    }

    /// Updates the `Notificator`'s frontiers from a `CountMap` per input.
    pub fn update_frontier_from_cm(&mut self, count_map: &mut [CountMap<T>]) {
        while self.frontier.len() < count_map.len() {
            self.frontier.push(MutableAntichain::new());
        }

        for (index, counts) in count_map.iter_mut().enumerate() {
            while let Some((time, delta)) = counts.pop() {
                self.frontier[index].update(&time, delta);
            }
        }
    }

    /// Reveals the elements in the frontier of the indicated input.
    pub fn frontier(&self, input: usize) -> &[T] {
        self.frontier[input].elements()
    }

    /// Requests a notification at the time associated with capability `cap`. Takes ownership of
    /// the capability.
    ///
    /// In order to request a notification at future timestamp, obtain a capability for the new
    /// timestamp first, as show in the example.
    ///
    /// #Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Unary};
    /// use timely::dataflow::channels::pact::Pipeline;
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .unary_notify(Pipeline, "example", Vec::new(), |input, output, notificator| {
    ///                while let Some((cap, data)) = input.next() {
    ///                    output.session(&cap).give_content(data);
    ///                    let mut time = cap.time();
    ///                    time.inner += 1;
    ///                    notificator.notify_at(cap.delayed(&time));
    ///                }
    ///                while let Some((cap, count)) = notificator.next() {
    ///                    println!("done with time: {:?}", cap.time());
    ///                }
    ///            });
    /// });
    /// ```
    #[inline]
    pub fn notify_at(&mut self, cap: Capability<T>) {
        self.pending.push(cap);
    }

    /// Repeatedly calls `logic` till exhaustion of the available notifications.
    ///
    /// `logic` receives a capability for `t`, the timestamp being notified and a `count`
    /// representing how many capabilities were requested for that specific timestamp.
    #[inline]
    pub fn for_each<F: FnMut(Capability<T>, i64)>(&mut self, mut logic: F) {
        while let Some((cap, count)) = self.next() {
            ::logging::log(&::logging::GUARDED_PROGRESS, true);
            logic(cap, count);
            ::logging::log(&::logging::GUARDED_PROGRESS, false);
        }
    }
}

impl<T: Timestamp> Iterator for Notificator<T> {
    type Item = (Capability<T>, i64);

    /// Retrieve the next available notification.
    ///
    /// Returns `None` if no notification is available. Returns `Some(cap, count)` otherwise:
    /// `cap` is a a capability for `t`, the timestamp being notified and, `count` represents
    /// how many capabilities were requested for that specific timestamp.
    fn next(&mut self) -> Option<(Capability<T>, i64)> {
        let found = self.pending.iter().enumerate().filter(|&(_pos, ref cap)| {
            let time = cap.time();
            !self.frontier.iter().any(|x| x.le(&time))
        }).fold(None /* (pos, time, count) */, |acc, (cur_pos, ref cur_cap)| {
            let cur_time = cur_cap.time();
            match acc {
                Some((acc_pos, acc_time, acc_count)) => {
                    if cur_time.lt(&acc_time) {
                        Some((cur_pos, cur_time, 1))
                    } else if cur_time.eq(&acc_time) {
                        Some((acc_pos, acc_time, acc_count + 1))
                    } else {
                        acc
                    }
                },
                None => Some((cur_pos, cur_time, 1)),
            }
        });
        found.map(|(pos, _, count)| (self.pending.swap_remove(pos), count))
    }
}


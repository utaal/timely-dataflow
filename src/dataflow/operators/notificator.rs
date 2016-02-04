use std::collections::VecDeque;
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
    pending: MutableAntichain<T>,
    capabilities: LazyKeyedSet<T, Capability<T>>,
    frontier: Vec<MutableAntichain<T>>,
    available: VecDeque<T>,
}

impl<T: Timestamp> Notificator<T> {
    pub fn new() -> Notificator<T> {
        Notificator {
            pending: Default::default(),
            capabilities: LazyKeyedSet::new(),
            frontier: Vec::new(),
            available: VecDeque::new(),
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
        self.pending.update(&cap.time(), 1);
        self.capabilities.add_item(cap.time(), cap);
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
    /// `cap` isa a capability for `t`, the timestamp being notified and, `count` represents
    /// how many capabilities were requested for that specific timestamp.
    fn next(&mut self) -> Option<(Capability<T>, i64)> {
        if self.available.len() == 0 {
            for pend in self.pending.elements().iter() {
                if !self.frontier.iter().any(|x| x.le(pend) ) {
                    self.available.push_back(pend.clone());
                }
            }
        }

        self.available.pop_front().map(|time| {
            let found = self.pending.count(&time)
                .and_then(|delta| self.capabilities.remove_item(&time).map(|c| (c, delta)));
            match found {
                Some((c, delta)) => {
                    self.pending.update(&time, -delta);
                    (c, delta)
                },
                None => panic!("failed to find available time in pending")
            }
        })
    }
}

/// A map-set hybrid which maintains all the (key, value) pairs provided until a certain key `k`
/// is retrieved via `remove_item(&k)`: this results in all `(k, _)` pairs being removed and only
/// one of them being returned by `remove_item`.
///
/// Useful as a stash for notificator's capabilities.
struct LazyKeyedSet<K: Eq, V> {
    vec: Vec<(K, V)>,
}

impl<K: Eq, V> LazyKeyedSet<K, V> {
    fn new() -> LazyKeyedSet<K, V> {
        LazyKeyedSet {
            vec: Vec::new(),
        }
    }

    fn add_item(&mut self, key: K, value: V) {
        self.vec.push((key, value));
    }

    fn remove_item(&mut self, key: &K) -> Option<V> {
        let mut last = 0;
        let mut found = None;
        while let Some(position) = (&self.vec[last..]).iter().position(|&(ref k, _)| k == key) {
            found = Some(self.vec.swap_remove(position + last));
            last = position;
        }
        found.map(|(_, v)| v)
    }
}

#[test]
fn lazy_keyed_set_should_behave_correctly() {
    let mut set = LazyKeyedSet::new();
    set.add_item(12, 12);
    set.add_item(12, 12);
    set.add_item(13, 13);
    set.add_item(14, 14);
    set.add_item(14, 14);
    assert!(set.vec.len() == 5);
    assert!(set.remove_item(&12).unwrap() == 12);
    assert!(set.vec.len() == 3);
    assert!(set.remove_item(&17).is_none());
    assert!(set.vec.len() == 3);
    assert!(set.remove_item(&14).unwrap() == 14);
    assert!(set.vec.len() == 1);
    assert!(set.remove_item(&13).unwrap() == 13);
    assert!(set.vec.len() == 0);
}


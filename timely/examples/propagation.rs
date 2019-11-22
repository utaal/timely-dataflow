extern crate timely;
extern crate log;

use timely::progress::frontier::Antichain;
use timely::progress::{Source, Target};
use timely::progress::reachability::Builder;

fn main() {
    env_logger::builder()
        .format_timestamp(None)
        .format_module_path(false)
        .init();

    // allocate a new empty topology builder.
    let mut builder = Builder::<usize>::new();

    // Each node with one input connected to one output.
    builder.add_node(0, 1, 1, vec![vec![Antichain::from_elem(0)]]);
    builder.add_node(1, 1, 1, vec![vec![Antichain::from_elem(0)]]);
    builder.add_node(2, 1, 1, vec![vec![Antichain::from_elem(1)]]);

    // Connect nodes in sequence, looping around to the first from the last.
    builder.add_edge(Source::new(0, 0), Target::new(1, 0));
    builder.add_edge(Source::new(1, 0), Target::new(2, 0));
    builder.add_edge(Source::new(2, 0), Target::new(0, 0));

    // Construct a reachability tracker.
    let (mut tracker, _) = builder.build();

    // Introduce a pointstamp at the output of the first node.
    tracker.update_source(Source::new(0, 0), 17, 1);

    // Propagate changes; until this call updates are simply buffered.
    tracker.propagate_all();
}
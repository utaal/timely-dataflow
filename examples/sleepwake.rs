extern crate timely;
extern crate timely_communication;

use std::sync::Arc;

use timely::dataflow::Stream;
use timely::dataflow::operators::*;
use timely::dataflow::operators::to_timely_stream::StreamInput;

use timely_communication::SleepWake;
use timely::sleepwake;
use timely::progress::Activity;
use timely::progress::timestamp::RootTimestamp;

fn main() {
    let sleep_wake = Arc::new(SleepWake::new());
    // initializes and runs a timely dataflow.
    timely::execute_from_args(std::env::args(), sleep_wake.clone(), move |worker| {
        let notifier = sleepwake::Notifier::new(sleep_wake.clone());
        let (sender, receiver) = notifier.new_async_channel();

        // create a new input, exchange data, and inspect its output
        let probe = worker.dataflow::<(), _, _>(move |scope| {
            let index = scope.index();
            let stream: Stream<_, u64> = receiver.to_timely_stream(scope, |capability| {
                let mut cap = Some(capability);
                move |input, output| {
                    match input {
                        StreamInput::Data(ds) => {
                            if ds.len() > 0 {
                                println!("call");
                            }
                            output.session(cap.as_ref().unwrap()).give_iterator(ds.iter().map(|x| *x));
                        },
                        StreamInput::Closed => cap = None,
                    }
                }
            });
            let probe = stream.exchange(|x| *x)
                              .inspect(move |x| println!("worker {}:\thello {}", index, x))
                              .probe();
            probe
        });

        if worker.index() == 0 {
            std::thread::spawn(move || {
                for i in 0..1000u64 {
                    println!("send: {}", i);
                    sender.send(i).expect("send");
                    if i % 10 == 0 {
                        std::thread::sleep(std::time::Duration::from_millis(1000));
                    }
                }
            });
        }

    }).unwrap();
}

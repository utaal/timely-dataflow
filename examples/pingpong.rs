#![feature(test)]

extern crate timely;
extern crate test;

use timely::dataflow::operators::*;

fn main() {

    let iterations = std::env::args().nth(1).unwrap().parse::<u64>().unwrap();
    let busy_loop = std::env::args().nth(2).unwrap().parse::<bool>().unwrap();

    // initializes and runs a timely dataflow
    timely::execute_from_args(std::env::args().skip(3), move |worker| {
        let index = worker.index();
        worker.dataflow(move |scope| {
            let (helper, cycle) = scope.loop_variable(iterations, 1);
            (0..1).take(if index == 0 { 1 } else { 0 })
                  .to_stream(scope)
                  .concat(&cycle)
                  // .inspect(|x| if x % 10000 == 0 { println!("📦 {}", x); })
                  .exchange(|&x| x)
                  .map_in_place(move |x| {
                      if busy_loop {
                          for k in (0..1000000000u64).map(|x| x * 2) {
                              ::test::black_box(k);
                          }
                      }
                      *x += 1;
                  })
                  .connect_loop(helper);
        });
    }).unwrap();
}

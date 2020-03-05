set terminal pdf size 6cm,4cm;
 set logscale x;
 set logscale y;
 set rmargin at screen 0.9;
 set bmargin at screen 0.2;
 set xrange [50000:5000000.0];
 # set yrange [0.005:1.01];
 set xlabel "nanoseconds";
 set format x "10^{%T}";
 set format y "10^{%T}";
 set ylabel "complementary cdf";
 set key left bottom Left reverse font ",10";
 plot \
    "data/barrier_master_1.cdf.cdf" using 1:2 with lines lw 2 dt (2, 6) title "baseline", \
    "data/barrier_nothing_1.cdf.cdf" using 1:2 with lines lw 2 dt (2, 6) title "disabled", \
    "data/barrier_timely_1.cdf.cdf" using 1:2 with lines lw 2 dt (2, 6) title "existing", \
    "data/barrier_tracker.cdf.cdf" using 1:2 with lines lw 2 dt (2, 6) title "existing + tracker"

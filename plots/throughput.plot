# Copyright 2020 Joyent, Inc.
#
# Assumes data file named 'chum.out' with data printed on a five-second
# interval.
#
datafile = 'chum.out'

set title "Throughput" font ",15"
set xlabel "Time" norotate font ",15"
set ylabel "Throughput (MB/s)" font ",15"

set xdata time
set timefmt "%s"
set format x "%b/%d %H:%M"
set xtics rotate by 45 right
set grid xtics ytics

to_mb(x) = (x/1024/1024/5)

plot \
datafile u 1:(to_mb($5)) w points pt 7 ps 0.5 lc rgb "blue" t "write", \
datafile u 1:(to_mb($4)) w points pt 7 ps 0.5 lc rgb "red" t "read", \
datafile u 1:(to_mb($5)+to_mb($4)) w points pt 7 ps 0.5 lc rgb "green" t "combined"

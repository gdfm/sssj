#!/bin/bash

cliopts="$@"	# optional parameters
max_procs=1	# parallelism
timeout=10800	# timeout in seconds

DATA="data/rcv1-seq.bin data/webspam-poi.bin data/blogs-06.bin data/tweets-06.bin"
THETA="0.5 0.6 0.7 0.8 0.9 0.99"
LAMBDA="0.1 0.01 0.001 0.0001"

#INDEX="INVERTED ALLPAIRS L2AP PUREL2AP"
INDEX="INVERTED L2AP PUREL2AP"
parallel --timeout ${timeout} --ungroup --max-procs ${max_procs} "scripts/minibatch {1} -t {2} -l {3} -i {4} ${cliopts} > results/{1/.}_t{2}_l{3}_MB-{4}" ::: $DATA ::: $THETA ::: $LAMBDA ::: $INDEX

#INDEX="INVERTED L2AP PUREL2AP"
INDEX="INVERTED L2AP PUREL2AP"
parallel --timeout ${timeout} --ungroup --max-procs ${max_procs} "scripts/streaming {1} -t {2} -l {3} -i {4} ${cliopts} > results/{1/.}_t{2}_l{3}_STR-{4}" ::: $DATA ::: $THETA ::: $LAMBDA ::: $INDEX

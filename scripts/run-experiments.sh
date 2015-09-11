#!/usr/local/bin/bash

cliopts="$@"
max_procs=1

DATA="data/RCV1_seq.bin"
THETA="0.35 0.5 0.7 0.8 0.9 0.99"
LAMBDA="1 0.1 0.01 0.001 0.0001"
INDEX="INVERTED ALL_PAIRS L2AP"

parallel --max-procs ${max_procs} "scripts/minibatch {1} -t {2} -l {3} -i {4} ${cliopts} > results/{1/.}_t{2}_l{3}_mb-{4}" ::: $DATA ::: $THETA ::: $LAMBDA ::: $INDEX
parallel --max-procs ${max_procs} "scripts/streaming {1} -t {2} -l {3} ${cliopts} > results/{1/.}_t{2}_l{3}_STREAMING" ::: $DATA ::: $THETA ::: $LAMBDA

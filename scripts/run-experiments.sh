#!/bin/bash

cliopts="$@"
max_procs=1
FORMAT="BINARY"
DATA="data/RCV1_seq.bin"

THETA="0.35 0.5 0.7 0.8 0.9 0.99"
LAMBDA="1 0.1 0.01 0.001 0.0001"
INDEX="INVERTED ALL_PAIRS L2AP"
parallel --max-procs ${max_procs} "scripts/minibatch -t {1} -l {2} -f ${FORMAT} ${DATA} ${cliopts}; echo" ::: $THETA ::: $LAMBDA > "results/t${t}_l${l}_mb-inv"
parallel --max-procs ${max_procs} "scripts/streaming -t {1} -l {2} -f ${FORMAT} ${DATA} ${cliopts}; echo" ::: $THETA ::: $LAMBDA > "results/t${t}_l${l}_str"

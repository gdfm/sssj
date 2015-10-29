#!/bin/bash

cliopts="$@"
max_procs=2

DATA="data/RCV1-seq.bin"
THETA="0.35 0.5 0.7 0.8 0.9 0.99"
LAMBDA="1 0.1 0.01 0.001 0.0001"

#INDEX="INVERTED ALLPAIRS L2AP"
#parallel --ungroup --max-procs ${max_procs} "scripts/minibatch {1} -t {2} -l {3} -i {4} ${cliopts} > results/{1/.}_t{2}_l{3}_MB-{4}" ::: $DATA ::: $THETA ::: $LAMBDA ::: $INDEX

INDEX="INVERTED PUREL2AP"
parallel --ungroup --max-procs ${max_procs} "scripts/streaming {1} -t {2} -l {3} -i {4} ${cliopts} > results/{1/.}_t{2}_l{3}_STR-{4}" ::: $DATA ::: $THETA ::: $LAMBDA ::: $INDEX

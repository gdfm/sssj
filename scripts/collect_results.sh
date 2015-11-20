#!/bin/bash

HEADER="algorithm, dataset, theta, lambda, index, time, entries, candidates, similarities, matches"
OUTFILE="results/results.csv"
echo $HEADER > $OUTFILE
tail -q -n 1 results/*_* | grep -v '~' >> $OUTFILE

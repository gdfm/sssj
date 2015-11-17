#!/bin/bash

HEADER="algorithm, dataset, theta, lambda, index, time, entries, candidates, similarities, matches"
echo $HEADER > results.csv
tail -q -n 1 results/* | grep -v '~' >> results.csv

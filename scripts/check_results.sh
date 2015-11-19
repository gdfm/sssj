#!/bin/bash

RES_DIR="results"
# results file name format: DATASET_THETA_LAMBDA_ALGO-INDEX
confs=$(ls $RES_DIR | grep '_' | cut -d '_' -f 1-3 | sort | uniq)
algos=$(ls $RES_DIR | grep '_' | cut -d '_' -f 4   | sort | uniq)

parallel -u -j0 "[[ \"{2}\" -eq \"{3}\" ]] || scripts/diff.py $RES_DIR/{1}_{2} $RES_DIR/{1}_{3}" ::: $confs ::: $algos ::: $algos

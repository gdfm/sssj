#!/bin/bash

RES_DIR="results"
# results file name format: DATASET_THETA_LAMBDA_ALGO-INDEX
confs=$(ls $RES_DIR | grep '_' | cut -d '_' -f 1-3 | sort | uniq)
#algos=$(ls $RES_DIR | grep '_' | cut -d '_' -f 4   | sort | uniq)
read -a algos <<< $(ls $RES_DIR | grep '_' | cut -d '_' -f 4   | sort | uniq) # array

files=""
for c in $confs; do
  for i in $(seq 0 $(( ${#algos[@]} - 1 )) ); do 
    for j in $( seq $i $(( ${#algos[@]} - 1 )) ); do
      [[ $i -eq $j ]] || files+="${RES_DIR}/${c}_${algos[i]} ${RES_DIR}/${c}_${algos[j]} "
    done
  done
done

parallel -u -m -j0 "scripts/diff.py {1} {2}" ::: $files

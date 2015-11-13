#!/usr/bin/env python
import sys
from itertools import chain
from collections import Counter

SEP = ':'

def process(line, fcount):
  tokens = [x for x in line.split() if SEP in x]
  features = set([int(x.split(SEP)[0]) for x in tokens])
  fcount.update(features)

def transform(line, mapping):
  tokens = line.split()
  header = ' '.join([x for x in tokens if not SEP in x])
  features = [x for x in tokens if SEP in x]
  dimensions = [int(x.split(SEP)[0]) for x in features]
  values = [x.split(SEP)[1] for x in features]
  new_dimensions = [str(mapping[x]) for x in dimensions]
  zipped = zip(new_dimensions, values)
  new_features = ' '.join([ SEP.join(chain(i)) for i in zipped])
  new_line =  header + ' ' + new_features
  return new_line


fcount = Counter()
for line in open(sys.argv[1]):
  process(line, fcount)

# fcount contains the feature frequency, now build a mapping
pairs = [(x[1],x[0]) for x in enumerate( [ y[0] for y in sorted(fcount.items(), reverse=True, key=lambda x: x[1]) ], 1 ) ]
mapping = dict(pairs)
  
for line in open(sys.argv[1]):
  newline = transform(line, mapping)
  print(newline)


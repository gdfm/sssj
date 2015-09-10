#!/usr/bin/env python
import sys

def main():
 p1 = sys.argv[1]
 p2 = sys.argv[2]
 s1 = load_result(p1)
 s2 = load_result(p2)
 if not s1 == s2:
   print "Results {0} {1} differ".format(p1, p2)
   diff = s1 ^ s2
   for i in diff:
     prefix = '<' if i in s1 else '>'
     print "{0}\t{1}".format(prefix, i)


def load_result(path):
  f = open(path)
  res = set()
  for line in f:
    if not ':' in line:
      continue
    tokens = line.split(':')
    v1 = eval(tokens[0].strip())
    d1 = eval(tokens[1].strip().replace('=',':'))
    for v2,s in d1.iteritems():
      tup = (min(v1,v2), max(v1,v2), s)
      res.add(tup)
  return res

if __name__ == '__main__':
    main()

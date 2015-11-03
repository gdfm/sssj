from __future__ import print_function
import sys
import json
import gzip
import re
import nltk
import time
import dateutil.parser
from collections import Counter
from bs4 import BeautifulSoup
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import TfidfVectorizer

def clean_text(text):
  words = text.encode("utf-8","ignore") # remove weird characters
#  words = BeautifulSoup(words.lower()).get_text() # remove HTML
  words = re.sub("[^a-z]", " ", words)  # keep letters only
  words = words.split() # tokenization
  words = [w for w in words if not w in stops] # remove stopwords
  return( " ".join(words)) # return single string

corpus = []
timestamps = []
stops = set(stopwords.words("english"))

for file in sys.argv[1:]:
  gfile = gzip.open(file)
  jsonobj = json.load(gfile)
  for post in jsonobj:
    if not post.get("language") == "en": # only english posts
      continue
    if post.get("text-en") == None: # ignore empty posts
      continue
    datestring = post.get("publishDate") # publish date goes back to 2000 (maybe too old)
    date = dateutil.parser.parse(datestring) # datestring to datetime
    ts = long(time.mktime(date.timetuple())) # datetime to Unix timestamp
#    date = post.get("fetchTime") # fetch time in ms (divide by 1000 to get the actual Unix time)
#    ts = long(date) / 1000 # Unix time (in seconds)
    text = post.get("text-en") # text of the blog post
    clean_post = clean_text(text)
    timestamps.append(ts)
    corpus.append(clean_post)

# ensure timestamps are unique
tscounts = Counter()
newts = []
for ts in timestamps:
  newts.append(ts * 1000 + tscounts[ts]) # convert to ms
  tscounts[ts] += 1 # add 1 ms of delay to each identical timestamp
timestamps = newts # ignore collisions (no collision unless there are >1000 pages with identical timestamp), check the output after!

# feature extraction
vectorizer = TfidfVectorizer(analyzer="word", max_features=100000, min_df=5, norm="l2")
features = vectorizer.fit_transform(corpus)
dataset = zip(timestamps, features)
print("Dataset statistics: {} x {} sparse matrix with {} non-zero elements".format(features.shape[0], features.shape[1], features.nnz), file=sys.stderr)

# print dataset
for (ts, vec) in sorted(dataset, key=lambda tup: tup[0]):
  vec.sort_indices()
  out = str(ts)  + " " + " ".join( [":".join([str(k),str(v)]) for k,v in zip(vec.indices, vec.data) ])
  print(out)

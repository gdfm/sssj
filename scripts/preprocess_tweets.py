from __future__ import print_function
import sys
import gzip
import re
import nltk
import time
import dateutil.parser
from collections import Counter
from bs4 import BeautifulSoup
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_extraction.text import HashingVectorizer

def clean_text(text):
  words = text.decode("utf-8", "ignore") # remove weird characters
#  words = BeautifulSoup(words.lower()).get_text() # remove HTML
  words = re.sub("[^a-z]", " ", words)  # keep letters only
  words = words.split() # tokenization
  words = [w for w in words if not w in stops] # remove stopwords
  return( " ".join(words)) # return single string

def output_batch(corpus, timestamps, tscounts, vectorizer):
  # ensure timestamps are unique
  newts = []
  for ts in timestamps:
    newts.append(ts * 1000 + tscounts[ts]) # convert to ms
    tscounts[ts] += 1 # add 1 ms of delay to each identical timestamp
  timestamps = newts # ignore collisions (no collision unless there are >1000 pages with identical timestamp), check the output after!

  features = vectorizer.fit_transform(corpus)
  dataset = zip(timestamps, features)
  print("Dataset statistics: {} x {} sparse matrix with {} non-zero elements".format(features.shape[0], features.shape[1], features.nnz), file=sys.stderr)
  
  # print dataset
  for (ts, vec) in sorted(dataset, key=lambda tup: tup[0]):
    vec.sort_indices()
    out = str(ts)  + " " + " ".join( [":".join([str(k),str(v)]) for k,v in zip(vec.indices, vec.data) ])
    print(out)


tscounts = Counter()
stops = set(stopwords.words("english"))
# feature extraction
#vectorizer = TfidfVectorizer(analyzer="word", max_features=100000, min_df=50, norm="l2")
vectorizer = HashingVectorizer(analyzer="word", non_negative=True, norm="l2") # n_features=2**20

corpus = []
timestamps = []

for file in sys.argv[1:]:
  gfile = gzip.open(file)
  for line in gfile:
    if line.startswith('T'):
      # process date
      datestring = line.lstrip('T\t')
      date = dateutil.parser.parse(datestring) # datestring to datetime
      ts = long(time.mktime(date.timetuple())) # datetime to Unix timestamp
      timestamps.append(ts)
    elif line.startswith('W'):
      # process tweet
      if line.find("No Post Title") != -1:
        timestamps.pop() # remove last added timestamp
        continue
      clean_tweet = clean_text(line.lstrip('W\t'))
      if len(clean_tweet) == 0:
        timestamps.pop()
        continue
      corpus.append(clean_tweet)
      if len(corpus) >= 10000:
        output_batch(corpus, timestamps, tscounts, vectorizer)
        corpus = []
        timestamps = []


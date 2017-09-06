
# coding: utf-8

# In[2]:

from pyspark.sql.types import StringType, ArrayType
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import HashingTF, IDF
from pyspark.sql.functions import udf
from pyspark.ml.clustering import LDA
import scipy.sparse as sps
from pyspark.mllib.linalg import Vectors

import re
import numpy as np
import pandas as pd
import nltk
from nltk.corpus import stopwords

from nltk import word_tokenize,sent_tokenize
nltk.download("popular")
nltk.download('punkt')

from gensim.models import doc2vec
from collections import namedtuple

from gensim.models import Doc2Vec
from collections import OrderedDict
import multiprocessing
from collections import namedtuple
from collections import defaultdict
from random import shuffle
import datetime
import gensim


# In[25]:


def normalise(word):
	"""Normalises words to lowercase and stems and lemmatizes it."""
	myPorterStemmer = nltk.stem.porter.PorterStemmer()
	word = word.lower()
	word = myPorterStemmer.stem(word)
	word = lemmatizer.lemmatize(word)
	return word

def acceptable_word(word):
	"""Checks conditions for acceptable word: length, stopword."""
	not_alphabet_regex = u"[^a-zA-Z]"
	#accepted = (2 <= len(word) <= 12) * (word.lower() not in stopwords) *(re.sub(not_alphabet_regex, " ", word) == word)
	accepted = bool(2 <= len(word) <= 12 and word.lower() not in stopwords and re.sub(not_alphabet_regex, " ", word) == word)
	return accepted

def read_stopwords(path):
    stopwords = []
    with open(path, "r") as file:
        for line in file:
            word = line.strip().lower()
            if len(word) != 0:
                stopwords.append(word)
    return list(stopwords)


# In[26]:

sqlContext = SQLContext(sc)


# In[30]:

lemmatizer = nltk.WordNetLemmatizer()
stemmer = nltk.stem.porter.PorterStemmer()

stopwords1 = read_stopwords("/home/datascience/enron/src/stopwords_eng.txt")
first_name = read_stopwords("/home/datascience/enron/src/CSV_Database_of_First_Names.csv")
last_name = read_stopwords("/home/datascience/enron/src/CSV_Database_of_Last_Names.csv")

stopwords = stopwords1  + first_name + last_name
stopwords = [s.lower() for s in stopwords]

not_alphabet_regex = u"[^a-zA-Z]"


# In[5]:

emails_rescaled_byauthor = sqlContext.read.format('parquet').load('/home/datascience/enron/src/emails_rescaled_byauthor')


# In[32]:

author_indx = emails_rescaled_byauthor.select("from").collect()


# In[7]:

model = Doc2Vec.load('/home/datascience/enron/Model/doc2vec_model3.txt')


# In[102]:

path = "/home/datascience/enron/src/mentee.txt"
mentee = []
with open(path, "r") as file:
    for line in file:
        words = line.split(",")
        words = [re.sub("\n", "", w).strip() for w in words]
        mentee = mentee + words
file.close()


# In[105]:

mentee = [w.lower() for w in mentee]
mentee


# In[51]:

path = "/home/datascience/enron/src/docs.csv"
docs_read = []
with open(path, "r") as file:
    for line in file:
        words = line.split(",")
        words = [re.sub("\n", "", w) for w in words]
        docs_read.append(words)
file.close()


# In[66]:

mentee_term = [normalise(w) for w in mentee if (2 <= len(w) <= 12) and (w not in stopwords) and re.sub(not_alphabet_regex, " ", w) == w]
new_vector = model.infer_vector(mentee)
similar_messages = model.docvecs.most_similar(positive = [new_vector])

similar_auth_message = [(author_indx[int(id)][0], docs_read[int(id)], float(sc)) for id, sc in similar_messages if sc > 0 ]
solution_df = pd.DataFrame(data=similar_auth_message, columns=['from', 'message', 'weight']).groupby('from').agg({'message':'sum', 'weight':'sum'})
solution_df = solution_df.sort_values(["weight"], ascending = 0)
t = min (len(solution_df.index), 5)
responce_indices = solution_df.index[range(0,t)]


# In[68]:

response = emails_rescaled_byauthor.rdd.filter(lambda x : x[0] in list(responce_indices))


# In[93]:

path = "/home/datascience/enron/Result/result.csv"
with open(path, "a") as myfile:
    for r in response.collect():
        features = pd.DataFrame(data=r[1], columns=['word', 'weight']).iloc[:,0].tolist()
        weights = pd.DataFrame(data=r[1], columns=['word', 'weight']).iloc[:,1].tolist()
        line0 = [r[0]] +  features + [str(w) for w in weights]
        line= ",".join(line0)
        myfile.write("%s\n" % line)
myfile.close()

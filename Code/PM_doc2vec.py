
# coding: utf-8

# In[15]:
from pyspark import SparkContext
sc = SparkContext("local", "Simple App")
from pyspark.sql.types import StringType, ArrayType
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import HashingTF, IDF
from pyspark.sql.functions import udf
from pyspark.ml.clustering import LDA
import scipy.sparse as sps
from pyspark.mllib.linalg import Vectors
from pyspark.sql import HiveContext

import sys, getopt
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


# In[7]:

def clean_email(s):
	#This funcion cleans the email extracting the body only
	#Parameters : s -sting, uncleaned body of email
	email_upperlimit =['Subject:', "X-FileName:"]
	email_lowerlimit =["----- Forwarded", "-----Original Message", "******************", "=============" ]
	email_upperlimit_pos = [s.find(i) for i in email_upperlimit]
	email_upperlimit_df = pd.DataFrame({'limit': email_upperlimit, 'position': email_upperlimit_pos})
	email_upperlimit_df = email_upperlimit_df.sort_values(['position'], ascending=False)
	email_upperlimit_df = email_upperlimit_df.loc[email_upperlimit_df['position'] >= 0]
	email_lowerlimit_pos = [s.find(i) for i in email_lowerlimit]
	email_lowerlimit2 = [i if i != "******************" else "\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*" for i in email_lowerlimit ]
	email_lowerlimit_df = pd.DataFrame({'limit': email_lowerlimit2, 'position': email_lowerlimit_pos})
	email_lowerlimit_df = email_lowerlimit_df.sort_values(['position'], ascending=True)
	email_lowerlimit_df = email_lowerlimit_df.loc[email_lowerlimit_df['position'] >= 0]
	body3 = ""
	if (email_upperlimit_df.shape[0] > 0):
		p1 = re.compile(email_upperlimit_df.iloc[0][0], re.IGNORECASE)
		if (email_lowerlimit_df.shape[0] > 0):
			if (email_upperlimit_df.iloc[0][1] < email_lowerlimit_df.iloc[0][1]):
				p2 = re.compile(email_lowerlimit_df.iloc[0][0], re.IGNORECASE)
				tmp = p1.split(s)
				body1 = p1.split(s)[1]
				body2 = p2.split(body1)[0]
				body3 = re.sub(' +',' ', body2)
			else:
				if (email_lowerlimit_df.shape[0] > 0):
					p2 = re.compile(email_lowerlimit_df.iloc[0][0], re.IGNORECASE)
					body2 = p2.split(s)[0]
					body3 = re.sub(' +',' ', body2)
	else:
		if (email_lowerlimit_df.shape[0] > 0):
			p2 = re.compile(email_lowerlimit_df.iloc[0][0], re.IGNORECASE)
			body2 = p2.split(s)[0]
			body3 = re.sub(' +',' ', body2)
		else: 
			body3 = s
	return body3


# COMMAND ----------



# COMMAND ----------

def execute(message):
	sentences = nltk.sent_tokenize(message)
	tree_list = []
	for sentence in sentences:
		tree_list.append(execute_sentence(sentence))
	return tree_list

def execute_sentence (sentence):
	chunker = nltk.RegexpParser(grammar)
	toks = nltk.word_tokenize(sentence)
	postoks = nltk.tag.pos_tag(toks)
	tree = chunker.parse(postoks)
	return tree

def leaves(tree):
	"""Finds NP (nounphrase) leaf nodes of a chunk tree."""
	for subtree in tree.subtrees(filter=lambda t: (t.label() == 'NP') or (t.label() == 'V') ):
		yield subtree.leaves()

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

def get_terms(tree):
	#for leaf in leaves(tree):
	terms = []
	for leaf in leaves(tree):
		term = [normalise(w) for w, t in leaf if acceptable_word(w)]
		if len(term)> 1 :
			terms.append(" ".join(term))
	return terms

def PhraseExtractor(message):
	trees = execute(message)
	terms = []
	for t in trees:
		terms.append(get_terms(t))	
	toks = nltk.word_tokenize(message)
	toks2 = [normalise(t) for t in toks if acceptable_word(t)]
	terms2 = [t for t in sum(terms, toks2) if t !=""] 
	return (terms2)

# COMMAND ----------

def decode_idf(vocab, v):
		v2 = v.toArray()
		v3 = [(vocab[i], v.toArray()[i]) for i in range(0,len(v.toArray())) if v.toArray()[i] > 0]
		return v3

# COMMAND ----------

def decode_vector_vocabulary (vocabulary):
	vocab = vocabulary
	def decode_vector( vector):
	#	vocabulary = CountVectorizerModel.vocabulary
		vector_decoded = [vocab[v] for v in vector]
		return (vector_decoded)
	return decode_vector

def read_stopwords(path):
    stopwords = []
    with open(path, "r") as file:
        for line in file:
            word = line.strip().lower()
            if len(word) != 0:                
                stopwords.append(word)
    return list(stopwords)


# In[8]:
"""

spark-submit ./Code/PM_doc2vec.py "/home/datascience/enron" "/Data/mail-2015.avro"  "/src/stopwords_eng.txt" "/src/CSV_Database_of_First_Names.csv" "/src/CSV_Database_of_Last_Names.csv"

path_global = "/home/datascience/enron"
path_data = path_global + "/Data/mail-2015.avro"
path_stopwords = path_global + "/src/stopwords_eng.txt"
path_firstnames = path_global + "/src/CSV_Database_of_First_Names.csv"
path_lastnames = path_global + "/src/CSV_Database_of_Last_Names.csv"
path_model = path_global + "/Model/doc2vec_model3.txt"
path_emails_rescaled_byauthor = path_global + "/src/emails_rescaled_byauthor"
path_docs = path_global + "/src/docs.csv"

"""
path_global = sys.argv[1]

path_data = path_global + sys.argv[2]
path_stopwords = path_global + sys.argv[3]
path_firstnames = path_global + sys.argv[4]
path_lastnames = path_global + sys.argv[5]
path_model = path_global + "/Model/doc2vec_model3.txt"
path_emails_rescaled_byauthor = path_global + "/src/emails_rescaled_byauthor"
path_docs = path_global + "/src/docs.csv"



sqlContext = HiveContext(sc)
emails = sqlContext.read.format("com.databricks.spark.avro").load(path_data)

emails_sent = emails.filter(emails["mailFields"]['FolderName'] == "sent_items")
emails_dedup = emails_sent.dropDuplicates(['from', 'body'])

udf_myFunction = udf(clean_email, StringType()) 
emails_dedup_cleaned = emails_dedup.withColumn("body_cleaned", udf_myFunction("body")) #"_3" being the column name of the column you want to consider
emails_dedup_cleaned_dedup = emails_dedup_cleaned.dropDuplicates(['from', 'body_cleaned'])


stopwords1 = read_stopwords(path_stopwords)
first_name = read_stopwords(path_firstnames)
last_name = read_stopwords(path_lastnames)

stopwords = stopwords1  + first_name + last_name
stopwords = [s.lower() for s in stopwords]

grammar = r"""
 NBAR:
		{<NN.*|JJ>*<NN.*>} # Nouns and Adjectives, terminated with Nouns
 NP:
		{<NBAR>}
		{<NBAR><IN><NBAR>} # Above, connected with in/of/etc...
 V: 
		{<V.*>} # Verb
 VP: 
		{<V> <NP|PP>*} # VP -> V (NP|PP)*
"""
lemmatizer = nltk.WordNetLemmatizer()
stemmer = nltk.stem.porter.PorterStemmer()

# COMMAND ----------

udf_PhraseExtractor = udf(PhraseExtractor, ArrayType(StringType())) 
emails_dedup_cleaned_dedup_chunk0 = emails_dedup_cleaned_dedup.withColumn("body_cleaned_chunk", udf_PhraseExtractor("body_cleaned"))
emails_dedup_cleaned_dedup_chunk = emails_dedup_cleaned_dedup_chunk0.select("from", "body_cleaned_chunk").rdd.filter(lambda x: len(x[1]) > 1).toDF(["from", "body_cleaned_chunk"]).cache()


# COMMAND ----------

#Version 1
cv = CountVectorizer(inputCol="body_cleaned_chunk", outputCol="rawFeatures", vocabSize=1000, minDF=10.0)
CountVectorizerModel = cv.fit(emails_dedup_cleaned_dedup_chunk)
emails_CountVectorized = CountVectorizerModel.transform(emails_dedup_cleaned_dedup_chunk).cache()

idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(emails_CountVectorized)
emails_rescaled = idfModel.transform(emails_CountVectorized).cache()


# In[16]:

docs = emails_dedup_cleaned_dedup_chunk.select("body_cleaned_chunk").collect()
author_indx = emails_dedup_cleaned_dedup_chunk.select("from").collect()
Doc2Vec_documents = []
analyzedDocument = namedtuple('analyzedDocument', 'words tags')

for i in range(0, len(docs)):
	Doc2Vec_documents.append(analyzedDocument(docs[i][0], str(i)))
    
    
doc_list = Doc2Vec_documents[:] # For reshuffling per pass


cores = multiprocessing.cpu_count()
assert gensim.models.doc2vec.FAST_VERSION > -1, "Speed enhansement"

simple_models = [
		# PV-DM w/ concatenation - window=5 (both sides) approximates paper's 10-word total window size
		Doc2Vec(dm=1, dm_concat=1, size=100, window=5, negative=5, hs=0, min_count=2, workers=cores),
		# PV-DBOW 
		Doc2Vec(dm=0, size=100, negative=5, hs=0, min_count=2, workers=cores),
		# PV-DM w/ average
		Doc2Vec(dm=1, dm_mean=1, size=100, window=10, negative=5, hs=0, min_count=2, workers=cores),
]	
	
simple_models[0].build_vocab(Doc2Vec_documents) # PV-DM w/ concat requires one special NULL word so it serves as template
print(simple_models[0])
for model in simple_models[1:]:
		model.reset_from(simple_models[0])
		print(model)

models_by_name = OrderedDict((str(model), model) for model in simple_models)

# COMMAND ----------

alpha, min_alpha, passes = (0.025, 0.001, 20)
alpha_delta = (alpha - min_alpha) / passes

print("START %s" % datetime.datetime.now())

for name, train_model in models_by_name.items():
	model.init_sims(replace=True)
	train_model.alpha, train_model.min_alpha = alpha, alpha
	train_model.train(doc_list, total_examples=len(doc_list), epochs=50)
	models_by_name[name] = train_model
	print ("Model " + name)
	#Test 1 
	str_pos = 'ga'
	print("most similar to:" + str_pos )
	print(train_model.most_similar(positive=[str_pos]))
	print ("\n")
	#Test 2 
	str_pos = 'energi'
	str_neg = 'trade'
	print("most similar to " + str_pos + " and least similar to " + str_neg )
	print(train_model.most_similar(positive=['energi'], negative=['trade'], topn=10))
	print ("\n")
	print("most similar to " + str_pos + " and " + str_neg )
	print(train_model.most_similar(positive=[str_pos, str_neg], topn=10))
	print ("\n")
	#Test 3 : Sanity Check
	rome_str = ['rome', 'italy']
	car_str = ['car']
	bool = train_model.docvecs.similarity_unseen_docs(train_model, rome_str, rome_str) > train_model.docvecs.similarity_unseen_docs(train_model, rome_str, car_str)
	print ("Sanity check " + str(bool))
	print ("\n\n\n")
	print("=================")



# In[17]:

models_by_name['Doc2Vec(dm/m,d100,n5,w10,mc2,s0.001,t8)'].save(path_model)


# In[25]:

vocab = CountVectorizerModel.vocabulary
emails_rescaled_decoded = emails_rescaled.select("from", "features").rdd.map(lambda v: (v[0], decode_idf(vocab, v[1])))
emails_rescaled_byauthor = emails_rescaled_decoded.reduceByKey(lambda a, b : a+b ).map(lambda x : (x[0], pd.DataFrame(data=x[1], columns=['word', 'weight']).groupby('word').agg({'weight':'sum'}).sort_values(["weight"], ascending = False)[:5].to_records(index=True).tolist())).toDF(["from", "features"])


# In[27]:

emails_rescaled_byauthor.write.save(path_emails_rescaled_byauthor, format='parquet', mode='overwrite')


# In[ ]:

docs2 = [d[0] for d in docs]

try:
	os.remove(path_docs)
except OSError:
	pass
	
with open(path_docs, "a") as myfile:
    for d in docs2:
        line = ",".join(d)
        myfile.write("%s\n" % line)

        
myfile.close()


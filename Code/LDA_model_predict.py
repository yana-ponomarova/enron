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
from gensim import corpora, models
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

def prep_text(text):
	not_alphabet_regex = u"[^a-zA-Z]"
	cleaned_text = re.sub(not_alphabet_regex, " ", text)
	cleaned_text_token = nltk.word_tokenize(cleaned_text)
	cleaned_text_token2 = [normalise(w) for w in cleaned_text_token if acceptable_word(w)]
	return cleaned_text_token2

# COMMAND ----------

def decode_idf(vocab, v):
		v2 = v.toArray()
		v3 = [(vocab[i], v.toArray()[i]) for i in range(0,len(v.toArray())) if v.toArray()[i] > 0]
		return v3

# COMMAND ----------

def decode_vector_vocabulary (vocabulary):
	vocab = vocabulary
	def decode_vector( vector):
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

def write_lda_model(K, train_set_lda, test_set_lda, dictionary, path_lda_models):
	ldamodel = gensim.models.ldamodel.LdaModel(train_set_lda, num_topics=K, id2word = dictionary, passes=20)
	lp = ldamodel.log_perplexity(test_set_lda)
	topics = ldamodel.show_topics()
	with open(path_lda_models, "a") as myfile:
		line = [str(K), str(lp)]
		for c in range(0, K) :
			line.append("topic " + str(topics[c][0]) + ": " + topics[c][1])
		myfile.write("%s\n" % line)
	
	myfile.close()
	return ldamodel
	
# In[8]:
"""

spark-submit ./Code/LDA_model_predict.py "/home/datascience/enron" "/Data/mail-2015.avro"  "/src/stopwords_eng.txt" "/src/CSV_Database_of_First_Names.csv" "/src/CSV_Database_of_Last_Names.csv"

path_global = "/home/datascience/enron"
path_data = path_global + "/Data/mail-2015.avro"
path_stopwords = path_global + "/src/stopwords_eng.txt"
path_firstnames = path_global + "/src/CSV_Database_of_First_Names.csv"
path_lastnames = path_global + "/src/CSV_Database_of_Last_Names.csv"
path_model = path_global + "/Model/doc2vec_model3.txt"
path_emails_rescaled_byauthor = path_global + "/src/emails_rescaled_byauthor"
path_docs = path_global + "/src/docs.csv"
path_emails_rescaled_byauthor = path_global + "/src/emails_rescaled_byauthor"
path_mentee = path_global + "/src/mentee.txt"
path_result_lda = path_global + "/Result/result_lda.csv"
path_ldamodel = path_global + "/Model/lda_model13.txt"
path_lda_dictionary = path_global + "/src/path_lda_dictionary.dic"
path_lda_corpora = path_global + '/src/path_lda_corpora.mm'
path_id_from = path_global + '/src/path_id_from.csv'


"""
path_global = sys.argv[1]
path_data = path_global + sys.argv[2]
path_stopwords = path_global + sys.argv[3]
path_firstnames = path_global + sys.argv[4]
path_lastnames = path_global + sys.argv[5]
path_model = path_global + "/Model/doc2vec_model3.txt"
path_emails_rescaled_byauthor = path_global + "/src/emails_rescaled_byauthor"
path_docs = path_global + "/src/docs.csv"
path_lda_models = path_global + "/LDA_models.txt"
path_mentee = path_global + "/src/mentee.txt"
path_emails_rescaled_byauthor = path_global + "/src/emails_rescaled_byauthor"
path_result_lda = path_global + "/Result/result_lda.csv"
path_ldamodel = path_global + "/Model/lda_model13.txt"
path_lda_dictionary = path_global + "/src/path_lda_dictionary.dic"
path_lda_corpora = path_global + '/src/path_lda_corpora.mm'
path_id_from = path_global + '/src/path_id_from.csv'

sqlContext = HiveContext(sc)


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


mentee = []
with open(path_mentee, "r") as file:
	for line in file:
		line.replace('"', '').strip()
		words = line.split(",")
		words = [re.sub("\n", "", w).strip() for w in words]
		mentee = mentee + words

file.close()

id_from = []
with open(path_id_from, "r") as file:
	for line in file:
		line.replace('"', '').strip()
		words = line.split(",")
		id_from.append(words)
		
file.close()
id_from = pd.DataFrame(data=id_from, columns=['id', 'from'])
	


ldamodel = models.ldamodel.LdaModel.load(path_ldamodel)
dictionary = corpora.Dictionary.load(path_lda_dictionary)
corpus = corpora.MmCorpus(path_lda_corpora)

mentee = [w.lower().replace('"', '').strip() for w in mentee if len(w) > 2]
mentee_vec = dictionary.doc2bow(mentee)
mentee_topic_vec = []
mentee_topic_vec = ldamodel[mentee_vec]

top2_topics_mentee = list(pd.DataFrame(data = mentee_topic_vec, columns=['word', 'weight']).sort_values(["weight"], ascending = False).iloc[:2,0])

similar_emails_mentee = []
for idx in range(len(corpus)):
	d = corpus[idx]
	p = [dict(ldamodel[d]).keys(), top2_topics_mentee]
	common_topics_d_mentee = set.intersection(*map(set, p))
	if len(common_topics_d_mentee) > 0 :
		#similar_emails_mentee.append((idx, {k: dict(ldamodel[d])[k] for k in dict(ldamodel[d]).keys() if k in common_topics_d_mentee}))
		topic_weight = [(idx, value) for key,value in dict(ldamodel[d]).items() if key in common_topics_d_mentee ]
		similar_emails_mentee = similar_emails_mentee + topic_weight
		
	
	
match_list = list(pd.DataFrame(data=similar_emails_mentee, columns=['idx', 'weight']).groupby('idx').agg({'weight':'sum'}).sort_values(["weight"], ascending = False)[:5].index)
author_match = set([id_from.ix[id_from['id']==str(a), 1].iloc[0] for a in match_list])

emails_rescaled_byauthor = sqlContext.read.format('parquet').load(path_emails_rescaled_byauthor)
response = emails_rescaled_byauthor.rdd.filter(lambda x : x[0] in author_match)

with open(path_result_lda, "a") as myfile:
    for r in response.collect():
        features = pd.DataFrame(data=r[1], columns=['word', 'weight']).iloc[:,0].tolist()
        weights = pd.DataFrame(data=r[1], columns=['word', 'weight']).iloc[:,1].tolist()
        line0 = [r[0]] +  features + [str(w) for w in weights]
        line= ",".join(line0)
        myfile.write("%s\n" % line)
			
		
myfile.close()



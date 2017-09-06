# enron
This is an application for mentors - mentees matching search.

We consider Enron employees as potential mentors and evaluate their competencies based in Enron Email dataset available here: http://www.cs.cmu.edu/~./enron/enron_mail_20150507.tgz,

A mentee is difined through a set of maximum 5 skills that he is interested in. Those can either be defined in a web-app interface or by filling in src/mentee.txt.

The project has the following structure:
- Code: 
- Model
- Result
- src : contains project dependencies stopwords and popular first and last names added to the stopwords
- webapp

Test: use the web-interface here http://193.70.6.96:8000/

In order to run the project:
Create "Data" folder in the root of the project, put mail-2015.avro there. The avro dataset mail-2015.avro has been obtained from the original Enron enron_mail_20150507.tgz conversion to avro using  https://github.com/medale/spark-mail tutorial.

Run 
1) Main model creation
spark-submit ./Code/PM_doc2vec.py "/home/datascience/enron" "/Data/mail-2015.avro"  "/src/stopwords_eng.txt" "/src/CSV_Database_of_First_Names.csv" "/src/CSV_Database_of_Last_Names.csv"

Arguments: 
path_global ()
path_data = "/Data/mail-2015.avro" - relative path to the avro datasource
path_stopwords = "/src/stopwords_eng.txt" - relative path to stopwords file
path_firstnames = "/src/CSV_Database_of_First_Names.csv" - relative path to first names file
path_lastnames = "/src/CSV_Database_of_Last_Names.csv" - relative path to last names file


Outputs created 
path_model = "/Model/doc2vec_model3.txt" - doc2vec model that will be used by mentor_predict.py
path_emails_rescaled_byauthor = '/src/emails_rescaled_byauthor' -Enron employees competencies as vectors, to be used by mentor_predict.py
path_docs = "/src/docs.csv" -doc2vec model that will be used by mentor_predict.py - corpus restructured, to be used by mentor_predict.py

2) Predic mentors
spark-submit ./Code/PM_doc2vec.py "/home/datascience/enron" "/Data/mail-2015.avro"  "/src/stopwords_eng.txt" "/src/CSV_Database_of_First_Names.csv" "/src/CSV_Database_of_Last_Names.csv"

Arguments: 
path_global ()
path_data = "/Data/mail-2015.avro" - relative path to the avro datasource
path_stopwords = "/src/stopwords_eng.txt" - relative path to stopwords file
path_firstnames = "/src/CSV_Database_of_First_Names.csv" - relative path to first names file
path_lastnames = "/src/CSV_Database_of_Last_Names.csv" - relative path to last names file
path_mentee =  "/src/mentee.txt" - relative path to  coma-separated list of mentee interests

Outputs created 
path_result = "/Result/result.csv" - file describing top 5 mentors

3) If you want to run the webapp, go here http://193.70.6.96:8000/
or, for a local installation, 
-install Django https://docs.djangoproject.com/fr/1.11/topics/install/
- got to ./webapp
- run $ python manage.py runserver



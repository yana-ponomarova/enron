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

In order to run the project:
Create "Data" folder in the root of the project, put mail-2015.avro there. The avro dataset mail-2015.avro has been obtained from the original Enron enron_mail_20150507.tgz conversion to avro using  https://github.com/medale/spark-mail tutorial.

src

from django.shortcuts import render
from django.http import HttpResponse
from django.http import HttpResponseRedirect
from form.models import Skill
import csv
import os
from . import forms
import codecs
from pathlib import Path
import time

# Create your views here.
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STATIC_DIR = os.path.join(BASE_DIR, "static")
CSV_DIR = os.path.join(STATIC_DIR, "csv/skills.csv")
DATA_RESPONSE = ""

def index(request):

    return render(request,'form/index.html')

def error(request):

    return render(request,'form/error.html')


def form_name_view(request):
    form = forms.FormName()

    # Check to see if we get a POST back.
    if request.method == 'POST':
        # In which case we pass in that request.
        form = forms.FormName(request.POST)

        # Check to see form is valid
        if form.is_valid():
            # Do something.
            choice1 = form.cleaned_data['skill1'].encode("utf-8")
            choice2 = form.cleaned_data['skill2'].encode("utf-8")
            choice3 = form.cleaned_data['skill3'].encode("utf-8")
            choice4 = form.cleaned_data['skill4'].encode("utf-8")
            choice5 = form.cleaned_data['skill5'].encode("utf-8")
            model = form.cleaned_data['model']
            choices_list = []
            choices_list.append(choice1)
            choices_list.append(choice2)
            choices_list.append(choice3)
            choices_list.append(choice4)
            choices_list.append(choice5)

            print choices_list
            print model


            with open("/home/datascience/enron/src/mentee.txt", "w") as f:
                choices_list_str = str(choices_list).replace("[","").replace("]","").replace("'","\"")
                f.write(choices_list_str)


            try:
                os.system('rm /home/datascience/enron/Result/result.csv')
            except:
                pass
            try:
                os.system('rm /home/datascience/enron/Result/result_lda.csv')
            except:
                pass


            if model == "1" :
                print("1st script launched")
                DATA_RESPONSE = "/home/datascience/enron/Result/result.csv"
                os.system("spark-submit --master local[4] /home/datascience/enron/Code/predict_mentors.py '/home/datascience/enron' '/src/stopwords_eng.txt' '/src/CSV_Database_of_First_Names.csv' '/src/CSV_Database_of_Last_Names.csv' '/src/mentee.txt'")

                while not os.path.exists(DATA_RESPONSE):
                    time.sleep(1)

                if os.path.isfile(DATA_RESPONSE):
                    return HttpResponseRedirect('/result')

                else:
                    raise ValueError("%s isn't a file!" % file_path)



            if model == "2" :
                print("2nd script launched")
                DATA_RESPONSE = "/home/datascience/enron/Result/result_lda.csv"
                os.system("spark-submit --master local[4] /home/datascience/enron/Code/LDA_model_predict.py '/home/datascience/enron' '/Data/mail-2015.avro' '/src/stopwords_eng.txt' '/src/CSV_Database_of_First_Names.csv' '/src/CSV_Database_of_Last_Names.csv'")

                while not os.path.exists(DATA_RESPONSE):
                    time.sleep(1)

                if os.path.isfile(DATA_RESPONSE):
                    return HttpResponseRedirect('/result-lda')

                else:
                    raise ValueError("%s isn't a file!" % file_path)

            else:
                print("Model not defined")


    return render(request,'form/selection.html',{'form':form})


def result(request):
    try:
        with open("/home/datascience/enron/Result/result.csv") as csvfile:
            readCSV = csv.reader(csvfile, delimiter=';')
            mentors = []
            for row in readCSV:
                mentors.append(row)
                for x in mentors:
                    mentors_list = list(str(mentors).split(','))


        mentor1_name = mentors_list[0].replace("[['","")
        mentor1_skill1 = mentors_list[1]
        mentor1_skill2 = mentors_list[2]
        mentor1_skill3 = mentors_list[3]
        mentor1_skill4 = mentors_list[4]
        mentor1_skill5 = mentors_list[5]
        mentor1_score1 = mentors_list[6]
        mentor1_score2 = mentors_list[7]
        mentor1_score3 = mentors_list[8]
        mentor1_score4 = mentors_list[9]
        mentor1_score5 = mentors_list[10].replace("']","")


        dict1 = {'mentor1_name':mentor1_name, 'mentor1_skill1':mentor1_skill1, 'mentor1_skill2':mentor1_skill2, 'mentor1_skill3':mentor1_skill3, 'mentor1_skill4':mentor1_skill4, 'mentor1_skill5':mentor1_skill5, 'mentor1_score1':mentor1_score1, 'mentor1_score2':mentor1_score2,'mentor1_score3':mentor1_score3,'mentor1_score4':mentor1_score4,'mentor1_score5':mentor1_score5,}

    except:
        dict1 = {}

    return render(request,'form/result.html',context=dict1)

def result2(request):
    try:
        with open("/home/datascience/enron/Result/result.csv") as csvfile:
            readCSV = csv.reader(csvfile, delimiter=';')
            mentors = []
            for row in readCSV:
                mentors.append(row)
                for x in mentors:
                    mentors_list = list(str(mentors).split(','))

        mentor2_name = mentors_list[11].replace("['","")
        mentor2_skill1 = mentors_list[12]
        mentor2_skill2 = mentors_list[13]
        mentor2_skill3 = mentors_list[14]
        mentor2_skill4 = mentors_list[15]
        mentor2_skill5 = mentors_list[16]
        mentor2_score1 = mentors_list[17]
        mentor2_score2 = mentors_list[18]
        mentor2_score3 = mentors_list[19]
        mentor2_score4 = mentors_list[20]
        mentor2_score5 = mentors_list[21].replace("']","")


        dict2 = {'mentor2_name':mentor2_name, 'mentor2_skill1':mentor2_skill1, 'mentor2_skill2':mentor2_skill2, 'mentor2_skill3':mentor2_skill3, 'mentor2_skill4':mentor2_skill4, 'mentor2_skill5':mentor2_skill5, 'mentor2_score1':mentor2_score1, 'mentor2_score2':mentor2_score2,'mentor2_score3':mentor2_score3,'mentor2_score4':mentor2_score4,'mentor2_score5':mentor2_score5,}

    except:
        dict2 = {}

    return render(request,'form/result2.html',context=dict2)

def result3(request):
    try:
        with open("/home/datascience/enron/Result/result.csv") as csvfile:
            readCSV = csv.reader(csvfile, delimiter=';')
            mentors = []
            for row in readCSV:
                mentors.append(row)
                for x in mentors:
                    mentors_list = list(str(mentors).split(','))

        mentor3_name = mentors_list[22].replace("['","")
        mentor3_skill1 = mentors_list[23]
        mentor3_skill2 = mentors_list[24]
        mentor3_skill3 = mentors_list[25]
        mentor3_skill4 = mentors_list[26]
        mentor3_skill5 = mentors_list[27]
        mentor3_score1 = mentors_list[28]
        mentor3_score2 = mentors_list[29]
        mentor3_score3 = mentors_list[30]
        mentor3_score4 = mentors_list[31]
        mentor3_score5 = mentors_list[32].replace("']","")


        dict3 = {'mentor3_name':mentor3_name, 'mentor3_skill1':mentor3_skill1, 'mentor3_skill2':mentor3_skill2, 'mentor3_skill3':mentor3_skill3, 'mentor3_skill4':mentor3_skill4, 'mentor3_skill5':mentor3_skill5, 'mentor3_score1':mentor3_score1, 'mentor3_score2':mentor3_score2,'mentor3_score3':mentor3_score3,'mentor3_score4':mentor3_score4,'mentor3_score5':mentor3_score5,}

    except:
        dict3 = {}

    return render(request,'form/result3.html',context=dict3)

def result4(request):

    try:
        with open("/home/datascience/enron/Result/result.csv") as csvfile:
            readCSV = csv.reader(csvfile, delimiter=';')
            mentors = []
            for row in readCSV:
                mentors.append(row)
                for x in mentors:
                    mentors_list = list(str(mentors).split(','))

        mentor4_name = mentors_list[33].replace("['","")
        mentor4_skill1 = mentors_list[34]
        mentor4_skill2 = mentors_list[35]
        mentor4_skill3 = mentors_list[36]
        mentor4_skill4 = mentors_list[37]
        mentor4_skill5 = mentors_list[38]
        mentor4_score1 = mentors_list[39]
        mentor4_score2 = mentors_list[40]
        mentor4_score3 = mentors_list[41]
        mentor4_score4 = mentors_list[42]
        mentor4_score5 = mentors_list[43].replace("']","")


        dict4 = {'mentor4_name':mentor4_name, 'mentor4_skill1':mentor4_skill1, 'mentor4_skill2':mentor4_skill2, 'mentor4_skill3':mentor4_skill3, 'mentor4_skill4':mentor4_skill4, 'mentor4_skill5':mentor4_skill5, 'mentor4_score1':mentor4_score1, 'mentor4_score2':mentor4_score2,'mentor4_score3':mentor4_score3,'mentor4_score4':mentor4_score4,'mentor4_score5':mentor4_score5,}
    except:
        dict4 = {}

    return render(request,'form/result4.html',context=dict4)

def result5(request):
    try:
        with open("/home/datascience/enron/Result/result.csv") as csvfile:
            readCSV = csv.reader(csvfile, delimiter=';')
            mentors = []
            for row in readCSV:
                mentors.append(row)
                for x in mentors:
                    mentors_list = list(str(mentors).split(','))

        mentor5_name = mentors_list[44].replace("['","")
        mentor5_skill1 = mentors_list[45]
        mentor5_skill2 = mentors_list[46]
        mentor5_skill3 = mentors_list[47]
        mentor5_skill4 = mentors_list[48]
        mentor5_skill5 = mentors_list[49]
        mentor5_score1 = mentors_list[50]
        mentor5_score2 = mentors_list[51]
        mentor5_score3 = mentors_list[52]
        mentor5_score4 = mentors_list[53]
        mentor5_score5 = mentors_list[54].replace("']]","")


        dict5 = {'mentor5_name':mentor5_name, 'mentor5_skill1':mentor5_skill1, 'mentor5_skill2':mentor5_skill2, 'mentor5_skill3':mentor5_skill3, 'mentor5_skill4':mentor5_skill4, 'mentor5_skill5':mentor5_skill5, 'mentor5_score1':mentor5_score1, 'mentor5_score2':mentor5_score2,'mentor5_score3':mentor5_score3,'mentor5_score4':mentor5_score4,'mentor5_score5':mentor5_score5,}

    except:
        dict5 = {}

    return render(request,'form/result5.html',context=dict5)


def result_lda(request):
    try:
        with open("/home/datascience/enron/Result/result_lda.csv") as csvfile:
            readCSV = csv.reader(csvfile, delimiter=';')
            mentors = []
            for row in readCSV:
                mentors.append(row)
                for x in mentors:
                    mentors_list = list(str(mentors).split(','))


        mentor1_name = mentors_list[0].replace("[['","")
        mentor1_skill1 = mentors_list[1]
        mentor1_skill2 = mentors_list[2]
        mentor1_skill3 = mentors_list[3]
        mentor1_skill4 = mentors_list[4]
        mentor1_skill5 = mentors_list[5]
        mentor1_score1 = mentors_list[6]
        mentor1_score2 = mentors_list[7]
        mentor1_score3 = mentors_list[8]
        mentor1_score4 = mentors_list[9]
        mentor1_score5 = mentors_list[10].replace("']","")

        dict1 = {'mentor1_name':mentor1_name, 'mentor1_skill1':mentor1_skill1, 'mentor1_skill2':mentor1_skill2, 'mentor1_skill3':mentor1_skill3, 'mentor1_skill4':mentor1_skill4, 'mentor1_skill5':mentor1_skill5, 'mentor1_score1':mentor1_score1, 'mentor1_score2':mentor1_score2,'mentor1_score3':mentor1_score3,'mentor1_score4':mentor1_score4,'mentor1_score5':mentor1_score5,}

    except:
        dict1 = {}

    return render(request,'form/result-lda.html',context=dict1)


def result_lda2(request):
    try:
        with open("/home/datascience/enron/Result/result_lda.csv") as csvfile:
            readCSV = csv.reader(csvfile, delimiter=';')
            mentors = []
            for row in readCSV:
                mentors.append(row)
                for x in mentors:
                    mentors_list = list(str(mentors).split(','))

        mentor2_name = mentors_list[11].replace("['","")
        mentor2_skill1 = mentors_list[12]
        mentor2_skill2 = mentors_list[13]
        mentor2_skill3 = mentors_list[14]
        mentor2_skill4 = mentors_list[15]
        mentor2_skill5 = mentors_list[16]
        mentor2_score1 = mentors_list[17]
        mentor2_score2 = mentors_list[18]
        mentor2_score3 = mentors_list[19]
        mentor2_score4 = mentors_list[20]
        mentor2_score5 = mentors_list[21].replace("']","")


        dict2 = {'mentor2_name':mentor2_name, 'mentor2_skill1':mentor2_skill1, 'mentor2_skill2':mentor2_skill2, 'mentor2_skill3':mentor2_skill3, 'mentor2_skill4':mentor2_skill4, 'mentor2_skill5':mentor2_skill5, 'mentor2_score1':mentor2_score1, 'mentor2_score2':mentor2_score2,'mentor2_score3':mentor2_score3,'mentor2_score4':mentor2_score4,'mentor2_score5':mentor2_score5,}

    except:
        dict2 = {}

    return render(request,'form/result-lda-2.html',context=dict2)

def result_lda3(request):
    try:
        with open("/home/datascience/enron/Result/result_lda.csv") as csvfile:
            readCSV = csv.reader(csvfile, delimiter=';')
            mentors = []
            for row in readCSV:
                mentors.append(row)
                for x in mentors:
                    mentors_list = list(str(mentors).split(','))

        mentor3_name = mentors_list[22].replace("['","")
        mentor3_skill1 = mentors_list[23]
        mentor3_skill2 = mentors_list[24]
        mentor3_skill3 = mentors_list[25]
        mentor3_skill4 = mentors_list[26]
        mentor3_skill5 = mentors_list[27]
        mentor3_score1 = mentors_list[28]
        mentor3_score2 = mentors_list[29]
        mentor3_score3 = mentors_list[30]
        mentor3_score4 = mentors_list[31]
        mentor3_score5 = mentors_list[32].replace("']","")


        dict3 = {'mentor3_name':mentor3_name, 'mentor3_skill1':mentor3_skill1, 'mentor3_skill2':mentor3_skill2, 'mentor3_skill3':mentor3_skill3, 'mentor3_skill4':mentor3_skill4, 'mentor3_skill5':mentor3_skill5, 'mentor3_score1':mentor3_score1, 'mentor3_score2':mentor3_score2,'mentor3_score3':mentor3_score3,'mentor3_score4':mentor3_score4,'mentor3_score5':mentor3_score5,}

    except:
        dict3 = {}

    return render(request,'form/result-lda-3.html',context=dict3)


def result_lda4(request):
    try:
        with open("/home/datascience/enron/Result/result_lda.csv") as csvfile:
            readCSV = csv.reader(csvfile, delimiter=';')
            mentors = []
            for row in readCSV:
                mentors.append(row)
                for x in mentors:
                    mentors_list = list(str(mentors).split(','))

        mentor4_name = mentors_list[33].replace("['","")
        mentor4_skill1 = mentors_list[34]
        mentor4_skill2 = mentors_list[35]
        mentor4_skill3 = mentors_list[36]
        mentor4_skill4 = mentors_list[37]
        mentor4_skill5 = mentors_list[38]
        mentor4_score1 = mentors_list[39]
        mentor4_score2 = mentors_list[40]
        mentor4_score3 = mentors_list[41]
        mentor4_score4 = mentors_list[42]
        mentor4_score5 = mentors_list[43].replace("']","")


        dict4 = {'mentor4_name':mentor4_name, 'mentor4_skill1':mentor4_skill1, 'mentor4_skill2':mentor4_skill2, 'mentor4_skill3':mentor4_skill3, 'mentor4_skill4':mentor4_skill4, 'mentor4_skill5':mentor4_skill5, 'mentor4_score1':mentor4_score1, 'mentor4_score2':mentor4_score2,'mentor4_score3':mentor4_score3,'mentor4_score4':mentor4_score4,'mentor4_score5':mentor4_score5,}

    except:
        dict4 = {}

    return render(request,'form/result-lda-4.html',context=dict4)


def result_lda5(request):
    try:
        with open("/home/datascience/enron/Result/result_lda.csv") as csvfile:
            readCSV = csv.reader(csvfile, delimiter=';')
            mentors = []
            for row in readCSV:
                mentors.append(row)
                for x in mentors:
                    mentors_list = list(str(mentors).split(','))

        mentor5_name = mentors_list[44].replace("['","")
        mentor5_skill1 = mentors_list[45]
        mentor5_skill2 = mentors_list[46]
        mentor5_skill3 = mentors_list[47]
        mentor5_skill4 = mentors_list[48]
        mentor5_skill5 = mentors_list[49]
        mentor5_score1 = mentors_list[50]
        mentor5_score2 = mentors_list[51]
        mentor5_score3 = mentors_list[52]
        mentor5_score4 = mentors_list[53]
        mentor5_score5 = mentors_list[54].replace("']]","")


        dict5 = {'mentor5_name':mentor5_name, 'mentor5_skill1':mentor5_skill1, 'mentor5_skill2':mentor5_skill2, 'mentor5_skill3':mentor5_skill3, 'mentor5_skill4':mentor5_skill4, 'mentor5_skill5':mentor5_skill5, 'mentor5_score1':mentor5_score1, 'mentor5_score2':mentor5_score2,'mentor5_score3':mentor5_score3,'mentor5_score4':mentor5_score4,'mentor5_score5':mentor5_score5,}

    except:
        dict5 = {}

    return render(request,'form/result-lda-5.html',context=dict5)

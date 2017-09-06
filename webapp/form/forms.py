from django import forms
import os
import csv
# Very Basic Example of a Django Form

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STATIC_DIR = os.path.join(BASE_DIR, "static")
CSV_DIR = os.path.join(STATIC_DIR, "csv/skills.csv")


class FormName(forms.Form):
    # choices = (('0',''),)
    # with open(CSV_DIR) as csvfile:
    #     readCSV = csv.reader(csvfile, delimiter=',')
    #     skills_list = []
    #     i = 0
    #     for row in readCSV:
    #         i = i + 1
    #         skill = row[0]
    #         skills_tuple = (str(i), skill)
    #         choices = choices + (skills_tuple,)

    #CHOICES = (('1', 'First',), ('2', 'Second',))

    skill1 = forms.CharField(max_length=50, required=False)
    skill2 = forms.CharField(max_length=50, required=False)
    skill3 = forms.CharField(max_length=50, required=False)
    skill4 = forms.CharField(max_length=50, required=False)
    skill5 = forms.CharField(max_length=50, required=False)

from django.conf.urls import url
from form import views

urlpatterns = [
    url(r'^form/',views.form_name_view,name='selection'),
    url(r'^error/',views.error,name='error'),
    url(r'^result/',views.result,name='result'),
    url(r'^result-2/',views.result2,name='result2'),
    url(r'^result-3/',views.result3,name='result3'),
    url(r'^result-4/',views.result4,name='result4'),
    url(r'^result-5/',views.result5,name='result5'),
    url(r'^result-lda/',views.result_lda,name='result_lda'),
    url(r'^result-lda-2/',views.result_lda2,name='result_lda2'),
    url(r'^result-lda-3/',views.result_lda3,name='result_lda3'),
    url(r'^result-lda-4/',views.result_lda4,name='result_lda4'),
    url(r'^result-lda-5/',views.result_lda5,name='result_lda5'),
]

"""enron URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/1.11/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  url(r'^$', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  url(r'^$', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.conf.urls import url, include
    2. Add a URL to urlpatterns:  url(r'^blog/', include('blog.urls'))
"""
from django.conf.urls import url
from django.contrib import admin
from django.conf.urls import include
from form import views

urlpatterns = [
    url(r'^$',views.index,name='index'),
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
    url(r'^admin/', admin.site.urls),
]

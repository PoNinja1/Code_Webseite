# device_overview/urls.py

from django.urls import path
from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('dataBase/', views.DataBaseView.as_view(), name='dataBase'),
    path('analysis/', views.AnalysisView.as_view(), name='analysis'),
]

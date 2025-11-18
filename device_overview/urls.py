# device_overview/urls.py

from django.urls import path
from .views import IndexView, UploadCsvView, DataBaseView, AnalysisView

urlpatterns = [
    path("", IndexView.as_view(), name="index"),
    path("upload-csv/", UploadCsvView.as_view(), name="upload_csv"),
    path("database/", DataBaseView.as_view(), name="dataBase"),
    path("analysis/", AnalysisView.as_view(), name="analysis"),
]

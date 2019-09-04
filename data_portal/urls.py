from django.contrib import admin
from django.urls import path

from data_portal import views

urlpatterns = [
    path('files', views.search_file, name='file-search'),
    path('file-signed-url', views.sign_s3_file, name='file-signed-url')
]

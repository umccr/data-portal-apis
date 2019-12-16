from django.urls import path, include
from rest_framework import routers

from data_portal import views
from .viewsets import LIMSRowViewSet

router = routers.DefaultRouter()
router.register(r'lims', LIMSRowViewSet)

urlpatterns = [
    path('', include(router.urls)),
    path('files', views.search_file, name='file-search'),
    path('file-signed-url', views.sign_s3_file, name='file-signed-url'),
    path('storage-stats', views.storage_stats, name='storage-stats')
]

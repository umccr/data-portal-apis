from django.urls import path, include
from rest_framework_nested import routers

from data_portal import views
from .viewsets import LIMSRowViewSet, S3ObjectViewSet, BucketViewSet, SubjectViewSet, SubjectS3ObjectViewSet, \
    RunViewSet, RunDataViewSet, RunMetaDataViewSet, PresignedUrlViewSet
from .routers import OptionalSlashDefaultRouter

router = OptionalSlashDefaultRouter()
router.register(r'lims', LIMSRowViewSet, basename='lims')
router.register(r's3', S3ObjectViewSet, basename='s3')
router.register(r'buckets', BucketViewSet, basename='buckets')
router.register(r'subjects', SubjectViewSet, basename='subjects')
router.register(r'runs', RunViewSet, basename='runs')
router.register(r'presign', PresignedUrlViewSet, basename='presign')

subjects_router = routers.NestedDefaultRouter(router, r'subjects', lookup='subject')
subjects_router.register(r's3', SubjectS3ObjectViewSet, basename='subject-s3')

runs_router = routers.NestedDefaultRouter(router, r'runs', lookup='run')
runs_router.register(r'lims', RunMetaDataViewSet, basename='run-lims')
runs_router.register(r's3', RunDataViewSet, basename='run-s3')

urlpatterns = [
    path('files', views.search_file, name='file-search'),
    path('file-signed-url', views.sign_s3_file, name='file-signed-url'),
    path('storage-stats', views.storage_stats, name='storage-stats'),
    path('', include(router.urls)),
    path('', include(subjects_router.urls)),
    path('', include(runs_router.urls)),
]

handler500 = 'rest_framework.exceptions.server_error'
handler400 = 'rest_framework.exceptions.bad_request'

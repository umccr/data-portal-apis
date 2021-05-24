from django.urls import path, include

from data_portal import views
from .routers import OptionalSlashDefaultRouter
from .viewsets import LIMSRowViewSet, S3ObjectViewSet, BucketViewSet, SubjectViewSet, \
    RunViewSet, PresignedUrlViewSet, GDSFileViewSet, ReportViewSet, LabMetadataViewSet

router = OptionalSlashDefaultRouter()
router.register(r'lims', LIMSRowViewSet, basename='lims')
router.register(r'metadata', LabMetadataViewSet, basename='metadata')
router.register(r's3', S3ObjectViewSet, basename='s3')
router.register(r'gds', GDSFileViewSet, basename='gds')
router.register(r'buckets', BucketViewSet, basename='buckets')
router.register(r'subjects', SubjectViewSet, basename='subjects')
router.register(r'runs', RunViewSet, basename='runs')
router.register(r'reports', ReportViewSet, basename='reports')
router.register(r'presign', PresignedUrlViewSet, basename='presign')

urlpatterns = [
    path('files', views.search_file, name='file-search'),
    path('file-signed-url', views.sign_s3_file, name='file-signed-url'),
    path('storage-stats', views.storage_stats, name='storage-stats'),
    path('', include(router.urls)),
]

handler500 = 'rest_framework.exceptions.server_error'
handler400 = 'rest_framework.exceptions.bad_request'

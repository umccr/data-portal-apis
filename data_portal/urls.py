from django.urls import path, include
from rest_framework_nested import routers

from data_portal import views
from .viewsets import LIMSRowViewSet, S3ObjectViewSet, BucketViewSet, SubjectViewSet, SubjectS3ObjectViewSet, \
    RunViewSet, PresignedUrlViewSet, RunDataLIMSViewSet, RunDataS3ObjectViewSet, SubjectGDSFileViewSet, \
    RunDataGDSFileViewSet, GDSFileViewSet, ReportViewSet, ReportSubjectViewSet
from .routers import OptionalSlashDefaultRouter

router = OptionalSlashDefaultRouter()
router.register(r'lims', LIMSRowViewSet, basename='lims')
router.register(r's3', S3ObjectViewSet, basename='s3')
router.register(r'gds', GDSFileViewSet, basename='gds')
router.register(r'buckets', BucketViewSet, basename='buckets')
router.register(r'subjects', SubjectViewSet, basename='subjects')
router.register(r'runs', RunViewSet, basename='runs')
router.register(r'reports', ReportViewSet, basename='reports')
router.register(r'presign', PresignedUrlViewSet, basename='presign')


subjects_router = routers.NestedDefaultRouter(router, r'subjects', lookup='subject')
subjects_router.register(r's3', SubjectS3ObjectViewSet, basename='subject-s3')
subjects_router.register(r'gds', SubjectGDSFileViewSet, basename='subject-gds')

runs_router = routers.NestedDefaultRouter(router, r'runs', lookup='run')
runs_router.register(r'lims', RunDataLIMSViewSet, basename='run-lims')
runs_router.register(r's3', RunDataS3ObjectViewSet, basename='run-s3')
runs_router.register(r'gds', RunDataGDSFileViewSet, basename='run-gds')

reports_router = routers.NestedDefaultRouter(router, r'reports', lookup='report')
reports_router.register(r'subject', ReportSubjectViewSet, basename='report-subject')

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

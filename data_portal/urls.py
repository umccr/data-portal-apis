from django.urls import path, include, re_path
from drf_yasg import openapi
from drf_yasg.views import get_schema_view
from rest_framework import permissions

from data_portal import views
from data_portal.routers import OptionalSlashDefaultRouter
from data_portal.viewsets.bucket import BucketViewSet
from data_portal.viewsets.fastqlistrow import FastqListRowViewSet
from data_portal.viewsets.gdsfile import GDSFileViewSet
from data_portal.viewsets.labmetadata import LabMetadataViewSet
from data_portal.viewsets.libraryrun import LibraryRunViewSet
from data_portal.viewsets.limsrow import LIMSRowViewSet
from data_portal.viewsets.pairing import PairingViewSet
from data_portal.viewsets.presignedurl import PresignedUrlViewSet
from data_portal.viewsets.run import RunViewSet
from data_portal.viewsets.s3object import S3ObjectViewSet
from data_portal.viewsets.sequence import SequenceViewSet
from data_portal.viewsets.sequencerun import SequenceRunViewSet
from data_portal.viewsets.somalier import SomalierViewSet
from data_portal.viewsets.subject import SubjectViewSet
from data_portal.viewsets.workflow import WorkflowViewSet
from data_portal.viewsets.manops import ManOpsViewSet

router = OptionalSlashDefaultRouter()
router.register(r'lims', LIMSRowViewSet, basename='lims')
router.register(r'metadata', LabMetadataViewSet, basename='metadata')
router.register(r's3', S3ObjectViewSet, basename='s3')
router.register(r'gds', GDSFileViewSet, basename='gds')
router.register(r'buckets', BucketViewSet, basename='buckets')
router.register(r'subjects', SubjectViewSet, basename='subjects')
router.register(r'runs', RunViewSet, basename='runs')
router.register(r'presign', PresignedUrlViewSet, basename='presign')

# ica pipeline workflow automation related endpoints
router.register(r'fastq', FastqListRowViewSet, basename='fastq')
router.register(r'sequencerun', SequenceRunViewSet, basename='sequencerun')
router.register(r'sequence', SequenceViewSet, basename='sequence')
router.register(r'libraryrun', LibraryRunViewSet, basename='libraryrun')
router.register(r'workflows', WorkflowViewSet, basename='workflows')
router.register(r'pairing', PairingViewSet, basename='pairing')
router.register(r'somalier', SomalierViewSet, basename='somalier')
router.register(r'manops', ManOpsViewSet, basename='manops')

schema_view = get_schema_view(
    openapi.Info(
        title="UMCCR Data Portal API",
        default_version='v1',
        description="UMCCR Data Portal API",
        terms_of_service="https://umccr.org/",
        contact=openapi.Contact(email="services@umccr.org"),
        license=openapi.License(name="MIT License"),
    ),
    public=True,
    permission_classes=(permissions.AllowAny,),
    patterns=[path('', include(router.urls)), ],
)

urlpatterns = [
    re_path(r'^swagger(?P<format>\.json|\.yaml)$', schema_view.without_ui(cache_timeout=0), name='schema-json'),
    re_path(r'^swagger/$', schema_view.with_ui('swagger', cache_timeout=0), name='schema-swagger-ui'),
    re_path(r'^redoc/$', schema_view.with_ui('redoc', cache_timeout=0), name='schema-redoc'),
    path('files', views.search_file, name='file-search'),  # TODO deprecate
    path('file-signed-url', views.sign_s3_file, name='file-signed-url'),  # TODO deprecate
    path('storage-stats', views.storage_stats, name='storage-stats'),  # TODO deprecate
    # we mirror the API surface at /iam/ - and set that path up in sls with an IAM authorizer as opposed to a JWT one
    path('iam/', include(router.urls)),
    # the main API surface authenticated using JWTs
    path('', include(router.urls)),
]

handler500 = 'rest_framework.exceptions.server_error'
handler400 = 'rest_framework.exceptions.bad_request'

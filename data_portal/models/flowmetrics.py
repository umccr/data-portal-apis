import logging
import uuid

from django.db import models
from django.db.models import QuerySet

from data_portal.fields import HashField, HashFieldHelper
from data_portal.models.labmetadata import LabMetadataPhenotype
from data_portal.models.base import PortalBaseModel, PortalBaseManager
from data_portal.models.gdsfile import GDSFile
from data_portal.models.s3object import S3Object

logger = logging.getLogger(__name__)


class FlowMetricsPhenoType(models.TextChoices):
    NORMAL = "normal"
    TUMOR = "tumor"

class FlowMetricsQCStatus(models.TextChoices):
    PASS = "pass"
    FAIL = "fail"
    NA = "na"

class FlowMetricsSex(models.TextChoices):
    MALE = "male"
    FEMALE = "female"
    NA = "na"

# class FlowMetricsTMB(models.TextChoices):
#     #FLOAT
#     NA = "na"

class FlowMetricsManager(PortalBaseManager):
# TODO: Also introduce *run_id* so that we can JOIN and relate with other foreign keys
    def get_by_unique_fields(
            self,
            subject_id: str,
            sample_id: str,
            #library_id: str,
    ) -> QuerySet:

        h = HashFieldHelper()
        h.add(subject_id).add(sample_id)
        #.add(library_id)

        return self.filter(unique_hash__exact=h.calculate_hash())

    def create_or_update_flowmetrics(
            self,
            subject_id: str,
            sample_id: str,
            #library_id: str,
    ):
        qs: QuerySet = self.get_by_unique_fields(
            subject_id=subject_id,
            sample_id=sample_id,
            #library_id=library_id,
        )

    def get_by_keyword(self, **kwargs) -> QuerySet:
        qs: QuerySet = super().get_queryset()
        return self.get_model_fields_query(qs, **kwargs)


class FlowMetrics(PortalBaseModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    portal_run_id = models.CharField(max_length=255, null=True)
    timestamp = models.DateTimeField()
    phenotype = models.CharField(choices=LabMetadataPhenotype.choices, max_length=255)
    cov_median_mosdepth = models.IntegerField()
    cov_auto_median_dragen = models.FloatField()
    reads_tot_input_dragen = models.BigIntegerField()
    reads_mapped_pct_dragen = models.FloatField()
    insert_len_median_dragen = models.IntegerField()
    var_tot_dragen = models.BigIntegerField()
    var_snp_dragen = models.FloatField()
    ploidy = models.FloatField(null=True) # Make sure NA is mapped to null at ORM level/ingestion
    purity = models.FloatField()
    qc_status_purple = models.CharField(choices=FlowMetricsQCStatus.choices, max_length=255)
    sex = models.CharField(choices=FlowMetricsSex.choices, max_length=255)
    ms_status = models.CharField(max_length=255)
    tmb = models.FloatField() # Encode NA?
    s3_object_id = models.CharField(max_length=255, null=True)
    gds_file_id = models.CharField(max_length=255, null=True)

    objects = FlowMetricsManager()

    def __str__(self):
        return f"ID: {self.id}"

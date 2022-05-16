import json
import logging

from django.db import models
from django.db.models import QuerySet

from data_portal.models.base import PortalBaseManager, PortalBaseModel
from data_portal.models.labmetadata import LabMetadata
from data_portal.models.sequencerun import SequenceRun

logger = logging.getLogger(__name__)


class FastqListRowManager(PortalBaseManager):

    def get_by_keyword(self, **kwargs) -> QuerySet:
        qs: QuerySet = super().get_queryset()

        run = kwargs.get('run', None)
        if run:
            qs = qs.filter(self.reduce_multi_values_qor('sequence_run__instrument_run_id', run))
            kwargs.pop('run')

        project_owner = kwargs.get('project_owner', None)
        if project_owner:
            q = self.reduce_multi_values_qor('project_owner', project_owner)
            qs_meta = LabMetadata.objects.filter(q).values("library_id")
            qs = qs.filter(rglb__in=qs_meta)
            kwargs.pop('project_owner')

        project_name = kwargs.get('project_name', None)
        if project_name:
            q = self.reduce_multi_values_qor('project_name', project_name)
            qs_meta = LabMetadata.objects.filter(q).values("library_id")
            qs = qs.filter(rglb__in=qs_meta)
            kwargs.pop('project_name')

        return self.get_model_fields_query(qs, **kwargs)


class FastqListRow(PortalBaseModel):
    class Meta:
        unique_together = ['rgid']

    id = models.BigAutoField(primary_key=True)
    rgid = models.CharField(max_length=255)
    rgsm = models.CharField(max_length=255)
    rglb = models.CharField(max_length=255)
    lane = models.IntegerField()
    read_1 = models.TextField()
    read_2 = models.TextField(null=True, blank=True)  # This is nullable. Search 'read_2' in fastq_list_row.handler()

    sequence_run = models.ForeignKey(SequenceRun, on_delete=models.SET_NULL, null=True, blank=True)

    objects = FastqListRowManager()

    def __str__(self):
        return f"RGID: {self.rgid}, RGSM: {self.rgsm}, RGLB: {self.rglb}"

    def as_dict(self):
        """as-is in dict i.e., no transformation"""
        d = {
            "rgid": self.rgid,
            "rglb": self.rglb,
            "rgsm": self.rgsm,
            "lane": self.lane,
            "read_1": self.read_1,
            "read_2": self.read_2
        }
        return d

    def as_json(self):
        return json.dumps(self.as_dict())

    def to_dict(self):
        dict_obj = {
            "rgid": self.rgid,
            "rglb": self.rglb,
            "rgsm": self.rgsm,
            "lane": self.lane,
            "read_1": self.read_1_to_dict()
        }
        if self.read_2:
            dict_obj["read_2"] = self.read_2_to_dict()
        return dict_obj

    def __json__(self):
        return json.dumps(self.to_dict())

    def read_1_to_dict(self):
        return {"class": "File", "location": self.read_1}

    def read_2_to_dict(self):
        return {"class": "File", "location": self.read_2}

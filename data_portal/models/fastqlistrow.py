import json
import logging

from django.db import models
from django.db.models import QuerySet

from data_portal.models.labmetadata import LabMetadata
from data_portal.models.sequencerun import SequenceRun
from django.core.exceptions import FieldError

from .utils import filter_object_by_parameter_keyword

logger = logging.getLogger(__name__)


class FastqListRowManager(models.Manager):

    def get_by_keyword(self, **kwargs) -> QuerySet:
        qs: QuerySet = self.all()

        SKIP_LIST = ["project_owner"]

        keywords = kwargs.get('keywords', None)
        if keywords:
            try:
                qs = filter_object_by_parameter_keyword(qs,keywords,SKIP_LIST )
            except FieldError:
                qs = self.none()

        project_owner = keywords.get('project_owner', None)
        if project_owner:
            qs_meta = LabMetadata.objects.filter(project_owner__iexact=project_owner).values("library_id")
            qs = qs.filter(rglb__in=qs_meta)

        return qs


class FastqListRow(models.Model):
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

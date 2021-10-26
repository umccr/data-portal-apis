import logging

from django.db import models

from data_portal.models.batch import Batch

logger = logging.getLogger(__name__)

class BatchRun(models.Model):
    id = models.BigAutoField(primary_key=True)
    batch = models.ForeignKey(Batch, on_delete=models.SET_NULL, null=True, blank=True)
    step = models.CharField(max_length=255)
    running = models.BooleanField(null=True, blank=True)
    notified = models.BooleanField(null=True, blank=True)

    def __str__(self):
        return f"ID: {self.id}, STEP: {self.step}, RUNNING: {self.running}, " \
               f"NOTIFIED: {self.notified}, BATCH_ID: {self.batch.id}"

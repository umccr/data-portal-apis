import logging

from django.db import models

logger = logging.getLogger(__name__)

class Batch(models.Model):
    class Meta:
        unique_together = ['name', 'created_by']

    id = models.BigAutoField(primary_key=True)
    name = models.CharField(max_length=255)
    created_by = models.CharField(max_length=255)
    context_data = models.TextField(null=True, blank=True)

    def __str__(self):
        return f"ID: {self.id}, NAME: {self.name}, CREATED_BY: {self.created_by}"

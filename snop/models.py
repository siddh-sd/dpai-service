from django.db import models
from com_scai_dpai.models import BaseModel
import uuid

class Snop(BaseModel):
    snop_id = models.UUIDField(default=uuid.uuid4, primary_key=True)
    snop_name = models.CharField(max_length=100)
    from_date = models.DateField(default=None)
    to_date = models.DateField(default=None)
    demand_review_date = models.DateField()
    supply_review_date = models.DateField()
    pre_snop_date = models.DateField()
    snop_date = models.DateField()
    forecast_trigger_date = models.DateField(null=True, blank=True)
    bu_id = models.CharField(max_length=100)

    def __str__(self):
        return str(self.snop_id)

    class Meta:
        indexes = [models.Index(fields=['bu_id', 'snop_id'])]
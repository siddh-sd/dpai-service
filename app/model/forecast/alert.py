from django.db import models
from app.model.forecast.forecast_header import ForecastHeader
from com_scai_dpai.models import BaseModel
import uuid

class Alert(BaseModel):
    alert_id = models.UUIDField(default=uuid.uuid4, primary_key=True)
    forecast_header_id_fk = models.ForeignKey(ForecastHeader, on_delete=models.CASCADE)
    alert_description = models.CharField(max_length=1000, blank=True)
    forecast_type = models.CharField(max_length=100)
    alert_type = models.CharField(max_length=100, default=None)
    user_level = models.IntegerField()
    read_by = models.CharField(max_length=100, null=True)
    alert_code = models.CharField(max_length=100, null=False)

    def __str__(self):
        return str(self.alert_id)
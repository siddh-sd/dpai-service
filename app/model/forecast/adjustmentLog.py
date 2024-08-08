from django.db import models
from app.model.forecast.forecast_header import ForecastHeader
from com_scai_dpai.models import BaseModel
import uuid

class AdjustmentLog(BaseModel):
    adjustment_log_id = models.UUIDField(default=uuid.uuid4, primary_key=True)
    forecast_header_id_fk = models.ForeignKey(ForecastHeader, on_delete=models.CASCADE)
    adjustment_log_description = models.CharField(max_length=1000)
    forecast_type = models.CharField(max_length=100)
    user_level = models.IntegerField()
    read_by = models.CharField(max_length=100, default='XYZ')

    def __str__(self):
        return str(self.adjustment_log_id)
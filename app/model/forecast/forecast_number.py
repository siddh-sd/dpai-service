from django.db import models
from com_scai_dpai.models import BaseModel
import uuid

class ForecastNumber(BaseModel):
    forecast_request_id = models.UUIDField(default=uuid.uuid4, primary_key=True)
    snop_id = models.CharField(max_length=200)
    forecast_number = models.CharField(max_length=200)
    forecast_status = models.CharField(max_length=200,default='In Progress')


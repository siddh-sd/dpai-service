from django.db import models
from app.model.forecast.forecast_header import ForecastHeader
from com_scai_dpai.models import BaseModel
import uuid


class ForecastDetail(BaseModel):
    forecast_detail_id = models.UUIDField(default=uuid.uuid4, primary_key=True)#readonly
    forecast_header_id_fk = models.ForeignKey(ForecastHeader, on_delete=models.CASCADE, related_name='header_detail')
    forecast_type = models.CharField(max_length=100)  # Operational, statistical #readonly
    period = models.DateField()#readonly
    volume = models.PositiveIntegerField(null=True)
    value = models.DecimalField(max_digits = 12, decimal_places = 2, null=True, blank=True)    # float

    def __str__(self):
        return str(self.forecast_detail_id)
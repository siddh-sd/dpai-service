from django.db import models
from snop.models import Snop
from com_scai_dpai.models import BaseModel
import uuid

class ForecastHeader(BaseModel):
    forecast_header_id = models.UUIDField(default=uuid.uuid4,primary_key=True)#readonly
    snop_id_fk = models.ForeignKey(Snop, on_delete=models.CASCADE)
    variability = models.CharField(max_length=100)#readonly
    sku_id = models.UUIDField(default=uuid.uuid4)    # sku_code/key_attribute_1#readonly
    node_id = models.UUIDField(default=uuid.uuid4)   # location_code/key_attribute_2#readonly
    segment = models.CharField(max_length=1, null=True)
    channel_id = models.UUIDField(default=uuid.uuid4)
    adi = models.DecimalField(max_digits = 10, decimal_places = 2, null=True, blank=True)
    cv = models.DecimalField(max_digits = 10, decimal_places = 2, null=True, blank=True)
    is_re_forecasted = models.BooleanField(default=False)
    sparsity = models.CharField(max_length=2, null=True)
    is_seasonal = models.BooleanField(default=False)
    fmr = models.CharField(max_length=2, null=True)
    remark_code = models.CharField(max_length=100, default="8-Others")
    remark_description = models.CharField(max_length=100, blank=True)
    operational_approved_till_level = models.IntegerField(default=0)
    sales_approved_till_level = models.IntegerField(default=0)
    unconstrained_approved_till_level = models.IntegerField(default=0)

    def __str__(self):
        return str(self.forecast_header_id)

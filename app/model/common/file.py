from django.db import models
from com_scai_dpai.models import BaseModel
import uuid
from snop.models import Snop

class File(BaseModel):
    file_id = models.UUIDField(default=uuid.uuid4, primary_key=True)
    file_type = models.CharField(max_length=100)
    file_name = models.CharField(max_length=1000)
    snop_id_fk = models.ForeignKey(Snop, on_delete=models.CASCADE)

    def __str__(self):
        return str(self.file_id)
import uuid
from django.db import models

class BaseModel(models.Model):
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    created_by = models.UUIDField(default=uuid.uuid4)
    updated_by = models.UUIDField(default=uuid.uuid4)
    
    class Meta:
        abstract = True
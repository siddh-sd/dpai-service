from django.db import models
from tenant_schemas.models import TenantMixin
import uuid
import os


class Client(TenantMixin):
    REQUIRED_FIELDS = ('tenant_id', 'schema_name')
    tenant_id = models.CharField(max_length=100, unique=True, null=False, blank=False)
    tenant_uuid = models.UUIDField(default=uuid.uuid4, null=False, blank=False)
    created_on = models.DateField(auto_now_add=True)
    domain_url = models.URLField(blank=True, null=True, default=os.getenv('DOMAIN'))
    schema_name = models.CharField(max_length=63, unique=True)
    # default true, schema will be automatically created and synced when it is saved
    auto_create_schema = True

from rest_framework import serializers
from tenant.models import Client
import logging

logger = logging.getLogger(__name__)


class TenantSerializer(serializers.ModelSerializer):
    class Meta:
        model = Client
        fields = ['tenant_id']

    def create(self, validated_data):
        schema = "scai_dpai_" + validated_data.get('tenant_id').replace("-", "_")
        return Client.objects.create(**validated_data, schema_name=schema)
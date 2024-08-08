from rest_framework import serializers
# from app.model.forecast.forecast_approval import ForecastApproval
from rest_framework.exceptions import ValidationError

import logging
logger = logging.getLogger(__name__)


class ForecastApprovalListSerializer(serializers.ListSerializer):
    def update(self, instances, validated_data):
        instance_hash = {index: instance for index, instance in enumerate(instances)}

        result = [
            self.child.update(instance_hash[index], attrs)
            for index, attrs in enumerate(validated_data)
        ]
        writable_fields = [x for x in self.child.Meta.fields if x not in self.child.Meta.read_only_fields]
        try:
            self.child.Meta.model.objects.bulk_update(result, writable_fields)
        except Exception as e:
            raise ValidationError(e)

        return result


# class ForecastApprovalSerializer(serializers.ModelSerializer):
#     class Meta:
#         model = ForecastApproval
#         list_serializer_class = ForecastApprovalListSerializer
#         fields = ['forecast_approval_id', 'forecast_header_id_fk',
#                   'forecast_type', 'approved_till_level', 'updated_at', 'updated_by', 'created_by', 'created_at']
#         read_only_fields = ["forecast_approval_id", "forecast_header_id_fk", 'forecast_type', 'created_at', 'created_by']
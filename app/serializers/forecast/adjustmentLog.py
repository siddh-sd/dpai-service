from rest_framework import serializers
from app.model.forecast.adjustmentLog import AdjustmentLog
from datetime import datetime
from rest_framework.exceptions import ValidationError
import logging

logger = logging.getLogger(__name__)

class AdjustmentLogListSerializer(serializers.ListSerializer):

    def create(self, validated_data):
        adjustmentLog_data = [AdjustmentLog(**item) for item in validated_data]
        return AdjustmentLog.objects.bulk_create(adjustmentLog_data)

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

    def to_representation(self, instances):
        rep_list = []
        for instance in instances:
            rep_list.append(
                dict(
                    adjustment_log_id = instance.pk,
                    forecast_header_id_fk = instance.forecast_header_id_fk_id,
                    adjustment_log_description = instance.adjustment_log_description,
                    forecast_type = instance.forecast_type,
                    user_level = instance.user_level,
                    created_at=instance.created_at,
                    updated_at=instance.updated_at,
                    created_by=instance.created_by,
                    updated_by=instance.updated_by
                )
            )
        return rep_list

class AdjustmentLogSerializer(serializers.ModelSerializer):

    class Meta:
        model = AdjustmentLog
        fields = '__all__'
        list_serializer_class = AdjustmentLogListSerializer
        read_only_fields = ["adjustment_log_id"]


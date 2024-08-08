from rest_framework import serializers
from datetime import datetime
from rest_framework.exceptions import ValidationError
import logging

logger = logging.getLogger(__name__)

class RemarkListSerializer(serializers.ListSerializer):

    def create(self, validated_data):
        ForecastRemarks_data = [Remark(**item) for item in validated_data]
        return Remark.objects.bulk_create(ForecastRemarks_data)

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
                    remark_id = instance.pk,
                    forecast_header_id_fk = instance.forecast_header_id_fk_id,
                    remark_code = instance.remark_code,
                    remark_description = instance.remark_description,
                    forecast_type = instance.forecast_type,
                    user_level = instance.user_level,
                    created_at=instance.created_at,
                    updated_at=instance.updated_at,
                    created_by=instance.created_by,
                    updated_by=instance.updated_by
                )
            )
        return rep_list



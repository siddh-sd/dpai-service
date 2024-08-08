from rest_framework import serializers
from app.model.forecast.alert import Alert
from datetime import datetime
from rest_framework.exceptions import ValidationError
import logging

logger = logging.getLogger(__name__)

class AlertListSerializer(serializers.ListSerializer):

    def create(self, validated_data):
        alerts_data = [Alert(**item) for item in validated_data]
        return Alert.objects.bulk_create(alerts_data)

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
                    alert_id = instance.pk,
                    forecast_header_id_fk = instance.forecast_header_id_fk_id,
                    alert_description = instance.alert_description,
                    alert_code = instance.alert_code,
                    forecast_type = instance.forecast_type,
                    alert_type = instance.alert_type,
                    user_level = instance.user_level,
                    created_at=instance.created_at,
                    updated_at=instance.updated_at,
                    created_by=instance.created_by,
                    updated_by=instance.updated_by,
                    read_by=instance.read_by
                )
            )
        return rep_list

class AlertSerializer(serializers.ModelSerializer):

    class Meta:
        model = Alert
        fields = '__all__'
        list_serializer_class = AlertListSerializer
        read_only_fields = ["alert_id"]


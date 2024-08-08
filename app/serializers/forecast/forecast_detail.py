from rest_framework import serializers
from app.model.forecast.forecast_detail import ForecastDetail
import logging
from rest_framework.exceptions import ValidationError

logger = logging.getLogger(__name__)

class ForecastDetailListSerializer(serializers.ListSerializer):

    def create(self, validated_data):
        ForecastDetail_data = [ForecastDetail(**item) for item in validated_data]
        return ForecastDetail.objects.bulk_create(ForecastDetail_data)

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
                    forecast_detail_id=instance.pk,
                    forecast_header_id_fk=instance.forecast_header_id_fk_id,
                    forecast_type=instance.forecast_type,
                    period=instance.period,
                    volume=instance.volume,
                    value=instance.value,
                    created_at=instance.created_at,
                    updated_at=instance.updated_at,
                    created_by=instance.created_by,
                    updated_by=instance.updated_by
                )
            )
        return rep_list


class ForecastDetailSerializer(serializers.ModelSerializer):
    forecast_type = serializers.CharField()
    period = serializers.DateField()
    created_at = serializers.DateTimeField()
    created_by = serializers.CharField()


    class Meta:
        model = ForecastDetail
        list_serializer_class = ForecastDetailListSerializer
        fields = ('forecast_detail_id', 'forecast_header_id_fk_id', 'volume', 'value', 'updated_at', 'updated_by', 'forecast_type', 'period', 'created_at', 'created_by')
        read_only_fields = ["forecast_detail_id"]


""" serializer for update forecastdetails"""

class ForecastDetailUpdateListSerializer(serializers.ListSerializer):
    def to_representation(self, instances):
        rep_list = []
        for instance in instances:
            rep_list.append(
                dict(
                    forecast_detail_id=instance.pk,
                    forecast_header_id_fk_id=instance.forecast_header_id_fk_id,
                    forecast_type=instance.forecast_type,
                    period=instance.period,
                    volume=instance.volume,
                    value=instance.value,
                    created_at=instance.created_at,
                    updated_at=instance.updated_at,
                    created_by=instance.created_by,
                    updated_by=instance.updated_by
                )
            )
        return rep_list


class ForecastDetailUpdateSerializer(serializers.ModelSerializer):
    forecast_type = serializers.CharField()
    period = serializers.DateField()

    class Meta:
        model = ForecastDetail
        list_serializer_class = ForecastDetailUpdateListSerializer
        fields = (
        'forecast_detail_id', 'forecast_header_id_fk_id', 'volume', 'value', 'updated_at', 'updated_by', 'forecast_type', 'period'
        )
        read_only_fields = ["forecast_detail_id"]

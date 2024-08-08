from rest_framework import serializers
from app.model.forecast.forecast_header import ForecastHeader
from rest_framework.exceptions import ValidationError
import logging

logger = logging.getLogger(__name__)

class ForecastHeaderListSerializer(serializers.ListSerializer):

    def create(self, validated_data):
        forecastHeader_data = [ForecastHeader(**item) for item in validated_data]
        return ForecastHeader.objects.bulk_create(forecastHeader_data)

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
                    forecast_header_id = instance.pk,
                    snop_id_fk = instance.snop_id_fk_id,
                    variability = instance.variability,
                    sku_id = instance.sku_id,
                    node_id = instance.node_id,
                    segment = instance.segment,
                    channel_id = instance.channel_id,
                    adi = instance.adi,
                    cv = instance.cv,
                    is_re_forecasted = instance.is_re_forecasted,
                    sparsity = instance.sparsity,
                    is_seasonal = instance.is_seasonal,
                    fmr = instance.fmr
                )
            )
        return rep_list

class ForecastHeaderSerializer(serializers.ModelSerializer):

    class Meta:
        model = ForecastHeader
        fields = (
            'forecast_header_id', 'snop_id_fk', 'variability', 'updated_by', 'created_by', 'created_at', 
            'sku_id', 'node_id', 'updated_at', 'segment', 'channel_id', 'adi', 'cv', 'is_re_forecasted', 'is_active', 'sparsity', 'is_seasonal', 'fmr')
        list_serializer_class = ForecastHeaderListSerializer
        read_only_fields = ["forecast_header_id"]
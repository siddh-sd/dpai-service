from rest_framework import serializers
from app.model.forecast.remark import RemarkMaster
from datetime import datetime

import logging

logger = logging.getLogger(__name__)


class RemarkMasterSerializer(serializers.ModelSerializer):

    class Meta:
        model = RemarkMaster
        fields = '__all__'



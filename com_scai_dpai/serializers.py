import math
from datetime import datetime, timedelta, date
from rest_framework import serializers
from com_scai_dpai.models import File

logger = logging.getLogger(__name__)

class FileSerializer(serializers.ModelSerializer):
    class Meta:
        model = File
        fields = '__all__'
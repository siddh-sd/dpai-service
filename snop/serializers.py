import math
from datetime import datetime, timedelta, date
from rest_framework import serializers
from snop.models import Snop
import time
from dateutil.relativedelta import relativedelta
import calendar
import logging
from com_scai_dpai.helper.configuration import Configuration
from snop.enum import SnopStatus, ResponseCodes, ConfigurationKeys, SnopConfigurationKeys
from com_scai_dpai.settings import DATE_FORMAT

logger = logging.getLogger(__name__)


class SnopSerializer(serializers.ModelSerializer):
    status = serializers.SerializerMethodField()

    class Meta:
        model = Snop
        fields = (
            'snop_id', 'snop_name', 'from_date', 'to_date', 'demand_review_date', 'supply_review_date', 'pre_snop_date',
            'snop_date', 'forecast_trigger_date', 'bu_id', 'updated_at', 'created_by', 'created_at', 'updated_by', 'status')

        def init(self, *args, **kwargs):
            super(SnopSerializer, self).init(*args, **kwargs)

    def get_status(self, obj):
        updated_at_date = datetime.now().date()
        if obj.snop_date >= updated_at_date and obj.from_date > updated_at_date:
            status = SnopStatus(1).name
        elif obj.from_date < updated_at_date:
            status = SnopStatus(2).name
        else:
            status = SnopStatus(3).name
        return status
    
    def validate(self, data):
        tenant_id = self.context.get('tenant_id')
        bu_id = data.get("bu_id")
        snop_id = data.get("snop_id")
        token = self.context.get('token')
        configuration = Configuration.get(tenant_id, bu_id, token)

        try:
            planningHorizon = configuration[ConfigurationKeys(1).name][SnopConfigurationKeys(3).name]
            planningCycleFrequency = configuration[ConfigurationKeys(1).name][SnopConfigurationKeys(2).name]
            weekStart = configuration[ConfigurationKeys(1).name][SnopConfigurationKeys(1).name]
        except Exception as e:
            logger.error({"SNOP VALIDATE: Invalid Configurations"})
            raise serializers.ValidationError(
                {"responseCode": ResponseCodes(5).name, "responseMessage": "Invalid Configurations"})

        from_date_request = data.get("from_date")
        from_date = time.strptime(str(data.get("from_date")), DATE_FORMAT)
        to_date = data.get("to_date")
        forecast_trigger_date = time.strptime(str(data.get("forecast_trigger_date")), DATE_FORMAT)
        today_date = time.strptime(str(date.today()), DATE_FORMAT)
        logger.debug(f'SNOP Todays date : {str(today_date)}')
        demand_review_date = time.strptime(str(data.get("demand_review_date")), DATE_FORMAT)
        supply_review_date = time.strptime(str(data.get("supply_review_date")), DATE_FORMAT)
        pre_snop_date = time.strptime(str(data.get("pre_snop_date")), DATE_FORMAT)
        snop_date = time.strptime(str(data.get("snop_date")), DATE_FORMAT)

        check_from_date_bu_id_isactive = self.check_from_date_bu_id_is_active(from_date_request, bu_id, snop_id)
        logger.debug(f'check_from_date_bu_id_isactive : {check_from_date_bu_id_isactive}')

        if check_from_date_bu_id_isactive is False:
            raise serializers.ValidationError(
                {"responseCode": ResponseCodes(6).name, "responseMessage": "SNOP already exist with same from date"})

        if snop_id:
            if forecast_trigger_date >= demand_review_date:
                raise serializers.ValidationError({"responseCode": ResponseCodes(19).name,
                                               "responseMessage": "Forecast Trigger date should be greater than current date and less than Demand Review date"})
        elif forecast_trigger_date <= today_date or forecast_trigger_date >= demand_review_date:
            raise serializers.ValidationError({"responseCode": ResponseCodes(19).name,
                                               "responseMessage": "Forecast Trigger date should be greater than current date and less than Demand Review date"})

        if from_date <= today_date:
            raise serializers.ValidationError({"responseCode": ResponseCodes(18).name,
                                               "responseMessage": "From date should be greater than current date"})

        if from_date < snop_date:
            raise serializers.ValidationError({"responseCode": ResponseCodes(17).name,
                                               "responseMessage": "SNOP date should be less than or equal to From date"})

        if demand_review_date >= supply_review_date:
            raise serializers.ValidationError({"responseCode": ResponseCodes(16).name,
                                               "responseMessage": "Demand Review date should be less than Supply Review date"})

        if supply_review_date >= pre_snop_date:
            raise serializers.ValidationError({"responseCode": ResponseCodes(15).name,
                                               "responseMessage": "Supply Review date should be less than Pre-SNOP date"})

        if pre_snop_date >= snop_date:
            raise serializers.ValidationError({"responseCode": ResponseCodes(14).name,
                                               "responseMessage": "Pre-SNOP date should be less than SNOP date"})
        self.validate_dates(planningCycleFrequency, from_date_request, to_date, planningHorizon, weekStart)

        return data

    def check_from_date_bu_id_is_active(self, from_date, bu_id, snop_id):
        snop_data = Snop.objects.filter(bu_id=bu_id, from_date=from_date, is_active=True)
        if snop_id:
            snop_data = snop_data.exclude(snop_id=snop_id)
        if len(snop_data) > 0:
            return False
        else:
            return True

    def validate_dates(self, planningCycleFrequency, from_date, to_date, planningHorizon, weekStart):
        if planningCycleFrequency.lower() == "daily":
            calculated_to_date = from_date + timedelta(days=planningHorizon)
            calculated_to_date = calculated_to_date - timedelta(days=1)
            if to_date == calculated_to_date:
                pass
            else:
                raise serializers.ValidationError({"responseCode": ResponseCodes(7).name,
                                                   "responseMessage": f"To Date should be {planningHorizon} days from from date"})

        elif planningCycleFrequency.lower() == "weekly":
            if weekStart.lower() != calendar.day_name[from_date.weekday()].lower():
                raise serializers.ValidationError({"responseCode": ResponseCodes(8).name,
                                                   "responseMessage": f"From date is {calendar.day_name[from_date.weekday()]}, it should be {weekStart} as per configuration"})

            calculated_to_date = from_date + timedelta(weeks=planningHorizon)
            calculated_to_date = calculated_to_date - timedelta(days=1)
            logger.debug(f"SNOP planningCycleFrequency :weeks: date should be : {str(calculated_to_date)}")
            if to_date != calculated_to_date:
                raise serializers.ValidationError({"responseCode": ResponseCodes(9).name,
                                                   "responseMessage": f"To Date should be {planningHorizon} weeks from from date"})

        elif planningCycleFrequency.lower() == "fortnightly":
            calculated_to_date = from_date + timedelta(days=planningHorizon * 15)
            calculated_to_date = calculated_to_date - timedelta(days=1)
            logger.debug(f"SNOP planningCycleFrequency forthnights to date should be : {str(calculated_to_date)}")
            if to_date != calculated_to_date:
                raise serializers.ValidationError({"responseCode": ResponseCodes(10).name,
                                                   "responseMessage": f"To Date should be {planningHorizon} fortnights from from date"})

        elif planningCycleFrequency.lower() == "monthly":
            firstDayOfMonth = date(from_date.year, from_date.month, 1)
            if firstDayOfMonth == from_date:
                calculated_to_date = firstDayOfMonth + relativedelta(months=planningHorizon)
                calculated_to_date = calculated_to_date - timedelta(days=1)
                logger.debug(f"SNOP planningCycleFrequency monthly date should be : {calculated_to_date}")
                if to_date != calculated_to_date:
                    raise serializers.ValidationError({"responseCode": ResponseCodes(11).name,
                                                       "responseMessage": f"To Date should be {planningHorizon} months from from date"})
            else:
                raise serializers.ValidationError({"responseCode": ResponseCodes(12).name,
                                               "responseMessage": "From date should starts from first of month"})

        elif planningCycleFrequency.lower() == "quarterly":
            start_of_quarter = datetime(year=from_date.year,
                                        month=((math.floor(((from_date.month - 1) / 3) + 1) - 1) * 3) + 1, day=1)
            start_of_quarter = datetime.strptime(str(start_of_quarter), "%Y-%m-%d %H:%M:%S")
            end_date = start_of_quarter + relativedelta(months=planningHorizon * 3)
            end_date = end_date - timedelta(days=1)
            logger.debug(f"Quarterly Initiated Expected: From date:{start_of_quarter} && End date: {end_date}")
            if start_of_quarter.date() == from_date and end_date.date() == to_date:
                pass
            else:
                raise serializers.ValidationError({"responseCode": ResponseCodes(13).name,
                                                   "responseMessage": f"To date should be {planningHorizon * 3} months from from date | from date should starts form first of next quarter"})

    def create(self, validated_data):
        return Snop.objects.create(**validated_data)

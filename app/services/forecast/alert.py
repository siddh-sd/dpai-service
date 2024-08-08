from com_scai_dpai.utils import Util as base_util
from app.utils import Util
from com_scai_dpai.helper.configuration import Configuration
from app.enum import ForecastConfigurationKeys, EntitiesConfigurationKeys, ForecastType, ResponseCodes, NetworkAttributeNames, ProductAttributeNames, LocationAttributeNames, SegmentationType, ForecastStatus, PersonnelAttributeNames, SnopConfigurationKeys, PlanningFrequencies, SalesHistoryAttributes, DataScienceForecastStatus, DataScienceGetForecastAttributes, AbbreviatedForecastType
from app.helper.entities.personnel import Personnel
from app.helper.entities.network import Network
from app.helper.entities.product import Product
from app.helper.entities.location import Location
from app.helper.transactional.salesHistory import SalesHistory
from rest_framework import status
from app.model.forecast.forecast_detail import ForecastDetail
from app.serializers.forecast.forecast_detail import ForecastDetailSerializer
import logging
from app.model.forecast.forecast_header import ForecastHeader
from snop.models import Snop
from django.db.models import Q
import pandas as pd
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta, date
from com_scai_dpai.helper.login import Login
from django.conf import settings
import json
import copy
import uuid
from django.core import serializers
import time
import numpy as np
from django.utils import timezone
from dateutil.parser import parse
from jsonmerge import merge
from app.model.forecast.alert import Alert
from django.db.models import F
from django.db.models.functions import Cast
from django.db.models import CharField
from app.serializers.forecast.alert import AlertSerializer

logger = logging.getLogger(__name__)

class AlertService():
    def get(self, tenant_id, bu_id, snop_id, token, forecast_type):
        try:
            sysToken = Login.get_token(settings.SYS_ADMIN_EMAIL, settings.SYS_ADMIN_PASSWORD)
            configurations = Configuration.get(tenant_id, bu_id, sysToken)
            if ((forecast_type.upper() == ForecastType(2).name or forecast_type.upper() == ForecastType(4).name) and configurations[ForecastConfigurationKeys(2).name][ForecastConfigurationKeys(5).name]) or forecast_type.upper() == ForecastType(3).name:
                snop = Snop.objects.get(snop_id=snop_id, bu_id=bu_id, is_active=True)
                if snop:
                    forecast_headers = ForecastHeader.objects.filter(snop_id_fk=snop_id, is_active=True)
                    '''personnel API integration'''
                    personnels = Personnel.get(tenant_id, bu_id, sysToken)

                    # fetch header ids whose user level > 0
                    if personnels and forecast_headers:
                        personelDataFrame = pd.DataFrame(personnels)
                        # personelDataFrame = personelDataFrame.drop(columns=['created_at'])
                        userEmail = base_util.getLoggedInUserEmailAddress(self, token)
                        if(forecast_type.upper() == ForecastType(2).name or forecast_type.upper() == ForecastType(4).name):
                            forecastUserHierarchyNoOfLevels = int(configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(1).name][EntitiesConfigurationKeys(2).name])
                            personelUserHierarchyKey = EntitiesConfigurationKeys(15).name
                        else:
                            forecastUserHierarchyNoOfLevels = int(configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(7).name][EntitiesConfigurationKeys(8).name])
                            personelUserHierarchyKey = EntitiesConfigurationKeys(16).name
                        
                        forecastDataFrame=pd.DataFrame.from_records(forecast_headers.values())
                        forecastDataFrame = forecastDataFrame.drop(columns=['created_at'])
                        # forecastDataFrame= pd.merge(forecastDataFrame, personelDataFrame, how="inner", left_on=["sku_id", "node_id", "channel_id"], right_on=["sku_id", "node_id", "channel_id"])
                        forecastDataFrame["sku_id"] = forecastDataFrame["sku_id"].map(lambda x: str(x))
                        forecastDataFrame["node_id"] = forecastDataFrame["node_id"].map(lambda x: str(x))
                        forecastDataFrame["channel_id"] = forecastDataFrame["channel_id"].map(lambda x: str(x))
                        if not forecastDataFrame.empty:
                            forecastDataFrame= pd.merge(forecastDataFrame, personelDataFrame, how="inner", left_on=["sku_id", "node_id", "channel_id"], right_on=["sku_id", "node_id", "channel_id"])
                        def userLevel(row):
                            user_level=0
                            for index in range(forecastUserHierarchyNoOfLevels, 0, -1):
                                if row[personelUserHierarchyKey + str(index)].upper() == userEmail.upper():
                                    user_level = index
                                    break
                            return user_level

                        forecastDataFrame["ul"] = forecastDataFrame.apply(userLevel, axis=1)
                        forecastDataFrame = forecastDataFrame[forecastDataFrame.ul > 0]
                        forecastHeaderIds = set(forecastDataFrame["forecast_header_id"])
                        forecastHeaderIds = list(forecastHeaderIds)
                        
                        #fetch records from alert table
                        alerts = Alert.objects.filter(forecast_header_id_fk_id__in=forecastHeaderIds, is_active=True, forecast_type = forecast_type)
                        if alerts:
                            alertDataFrame = pd.DataFrame.from_records(alerts.values())
                            alertDataFrame = pd.merge(forecastDataFrame, alertDataFrame, how="inner", left_on=["forecast_header_id"], right_on=["forecast_header_id_fk_id"])
                            alertDataFrame['ir'] = np.where(alertDataFrame['read_by'].isnull() | (~alertDataFrame['read_by'].apply(lambda x: x is not None and any(level in x.split(',') for level in forecastDataFrame['ul'].astype(str)))), False, True)
                            alertDataFrame = alertDataFrame[['alert_description', 'created_at', 'alert_id', 'ir', 'user_level', 'forecast_header_id_fk_id', 'alert_type']]
                            alertDataFrame = alertDataFrame.rename(columns={'alert_description': 'ld', 'created_at': 'ca', 'alert_id': 'id', 'ir': 'ir', 'user_level': 'ul', 'forecast_header_id_fk_id': 'fhi', 'alert_type': 'at'})
                            sorted_alertDataFrame = alertDataFrame.sort_values(by='ir', ascending=True)
                            alerts = json.loads(sorted_alertDataFrame.to_json(orient='records', default_handler=str, date_format='iso'))

                            return {"responseCode":ResponseCodes(27).name,"responseMessage": "Successfull", "status": status.HTTP_200_OK, "data":alerts}    
                        else:
                            return {"responseCode":ResponseCodes(27).name,"responseMessage": "Successfull", "status": status.HTTP_200_OK, "data":[]}    
                    else:
                        logger.error(f"DPAI Service: {forecast_type}AlertService-GET: Personnel Data not available: tenant_id: {tenant_id} | bu_id: {bu_id} | personnelAPIResponse: {personnels}")
                        return {"responseCode":ResponseCodes(4).name,"responseMessage": "Personnel Data not available", "status": status.HTTP_400_BAD_REQUEST}
                else:
                    logger.error(f"DPAI Service: {forecast_type}AlertService-GET: Invalid SNOP ID: tenant_id: {tenant_id} | bu_id: {bu_id} | snop_id: {snop_id}")
                    return {"responseCode":ResponseCodes(6).name,"responseMessage": "Invalid Request Body", "status": status.HTTP_400_BAD_REQUEST}
            else:
                logger.error(f"DPAI Service: {forecast_type}AlertService-GET: Invalid Request: tenant_id: {tenant_id} | bu_id: {bu_id} | snop_id: {snop_id}")
                return {"responseCode":ResponseCodes(5).name,"responseMessage": "Invalid Request", "status": status.HTTP_400_BAD_REQUEST}
        except Exception as e:
            logger.error(f"DPAI Service: {forecast_type}AlertService-GET: Alert data not available: {e}")
            return {"responseCode":ResponseCodes(26).name,"responseMessage": "Alert not available!", "status": status.HTTP_400_BAD_REQUEST}
    
    def update(self, *args, **kwargs):
        try:
            tenant_id, bu_id, snop_id, token, forecast_type, data = kwargs['tenant_id'], kwargs['bu_id'], kwargs['snop_id'], kwargs['token'], kwargs['forecast_type'], kwargs['data']
            configurations = Configuration.get(tenant_id, bu_id, token)
            if((forecast_type.upper() == ForecastType(2).name or forecast_type.upper() == ForecastType(4).name) and configurations[ForecastConfigurationKeys(2).name][ForecastConfigurationKeys(5).name]) or forecast_type.upper() == ForecastType(3).name:
                snop = Snop.objects.get(snop_id=snop_id, bu_id=bu_id, is_active=True)

                # Personal Api Integration
                personnels = Personnel.get(tenant_id, bu_id, token)
                forecast_headers = ForecastHeader.objects.filter(snop_id_fk=snop_id, is_active=True)
                if snop and personnels and forecast_headers:
                    userEmail = base_util.getLoggedInUserEmailAddress(self, token)
                    if(forecast_type.upper() == ForecastType(2).name or forecast_type.upper() == ForecastType(4).name):
                        forecastUserHierarchyNoOfLevels = int(configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(1).name][EntitiesConfigurationKeys(2).name])
                        personelUserHierarchyKey = EntitiesConfigurationKeys(15).name
                    else:
                        forecastUserHierarchyNoOfLevels = int(configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(7).name][EntitiesConfigurationKeys(8).name])
                        personelUserHierarchyKey = EntitiesConfigurationKeys(16).name

                    # create Personnels DataFrame
                    personnelsDataFrame = pd.DataFrame(personnels)
                    # create Forcast DataFrame
                    forecastDataFrame=pd.DataFrame.from_records(forecast_headers.values())
                    forecastDataFrame = forecastDataFrame.drop(columns=['created_at'])
                    forecastDataFrame["sku_id"] = forecastDataFrame["sku_id"].map(lambda x: str(x))
                    forecastDataFrame["node_id"] = forecastDataFrame["node_id"].map(lambda x: str(x))
                    forecastDataFrame["channel_id"] = forecastDataFrame["channel_id"].map(lambda x: str(x))

                    def userLevel(row):
                        user_level=0
                        for index in range(forecastUserHierarchyNoOfLevels, 0, -1):
                            if row[personelUserHierarchyKey + str(index)].upper() == userEmail.upper():
                                user_level = index
                                break
                        return user_level

                    if not personnelsDataFrame.empty and not forecastDataFrame.empty:
                        forecastDataFrame= pd.merge(forecastDataFrame, personnelsDataFrame, how="inner", left_on=["sku_id", "node_id", "channel_id"], right_on=["sku_id", "node_id", "channel_id"])                    
                        forecastDataFrame["user_level"] = forecastDataFrame.apply(userLevel, axis=1)
                        forecastDataFrame = forecastDataFrame[forecastDataFrame.user_level > 0]
                        forecastHeaderIds = list(forecastDataFrame["forecast_header_id"].unique())
                    else:
                        logger.error(f"DPAI Service: {forecast_type}AlertService-GET: Personnel Data not available: tenant_id: {tenant_id} | bu_id: {bu_id} | personnelAPIResponse: {personnels}")
                        return {"responseCode":ResponseCodes(4).name,"responseMessage": "Personnel Data not available", "status": status.HTTP_400_BAD_REQUEST}

                    alerts = Alert.objects.filter(forecast_header_id_fk_id__in=forecastHeaderIds, is_active=True, forecast_type = forecast_type)
                    if alerts:
                        alertDataFrame = pd.DataFrame.from_records(alerts.values())
                        alertDataFrame = pd.merge(forecastDataFrame, alertDataFrame, how="inner", left_on=["forecast_header_id"], right_on=["forecast_header_id_fk_id"])
                        alertDataFrame['read_by'] = ','.join(set(alertDataFrame['user_level_y'].astype(str)))
                        alertDataFrame['updated_at_y'] = timezone.now()
                        alertDataFrame['updated_by_y'] = base_util.getLoggedInUserId(token)
                        alertDataFrame = alertDataFrame[['is_active_y', 'created_at', 'updated_at_y', 'created_by_y', 'updated_by_y','alert_id', 'alert_description', 'alert_code', 'alert_type','forecast_type', 'user_level_y', 'forecast_header_id', 'read_by']]
                        alertDataFrame = alertDataFrame.rename(columns={'is_active_y':'is_active', 'created_by_y':'created_by', 'updated_by_y':'updated_by', 'user_level_y':'user_level', 'updated_at_y':'updated_at'})

                        for _, row in alertDataFrame.iterrows():
                            data = {
                                'is_active': row['is_active'],
                                'created_at': row['created_at'],
                                'updated_at': row['updated_at'],
                                'created_by': row['created_by'],
                                'updated_by': row['updated_by'],
                                'alert_id': row['alert_id'],
                                'alert_description': row['alert_description'],
                                'alert_code': row['alert_code'],
                                'alert_type': row['alert_type'],
                                'forecast_type': row['forecast_type'],
                                'user_level': row['user_level'],
                                'forecast_header_id':row['forecast_header_id'],
                                'read_by': row['read_by']
                            }
                            adjustment_log, _ = Alert.objects.update_or_create(alert_id=row['alert_id'], defaults=data)
                        return {"responseCode":ResponseCodes(27).name,"responseMessage": "Successfull", "status": status.HTTP_200_OK, "data":data}    
                    else:
                        return {"responseCode":ResponseCodes(5).name,"responseMessage": "Invalid Request", "status": status.HTTP_400_BAD_REQUEST, "data":[]}    
                else:
                    logger.error(f"DPAI Service: {forecast_type}AlertService-GET: Invalid SNOP ID: tenant_id: {tenant_id} | bu_id: {bu_id} | snop_id: {snop_id}")
                    return {"responseCode":ResponseCodes(6).name,"responseMessage": "Invalid Request Body", "status": status.HTTP_400_BAD_REQUEST}
            else:
                logger.error(f"DPAI Service: {forecast_type}AlertService-GET: Invalid Request: tenant_id: {tenant_id} | bu_id: {bu_id} | snop_id: {snop_id}")
                return {"responseCode":ResponseCodes(5).name,"responseMessage": "Invalid Request", "status": status.HTTP_400_BAD_REQUEST}
        except Exception as e:
            logger.error(f"DPAI Service: {forecast_type}AlertService-GET: Alert data not available: {e}")
            return {"responseCode":ResponseCodes(26).name,"responseMessage": "Alert not available!", "status": status.HTTP_400_BAD_REQUEST}


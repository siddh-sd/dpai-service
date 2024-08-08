import os
import brotli
import zstandard as zstd
from io import StringIO
from app.helper.cache.forecast import ForecastCache
from com_scai_dpai.utils import Util as base_util
from app.utils import Util
from com_scai_dpai.helper.configuration import Configuration
from app.enum import ForecastConfigurationKeys, EntitiesConfigurationKeys, ForecastType, ResponseCodes, NetworkAttributeNames, ProductAttributeNames, LocationAttributeNames, SegmentationType, ForecastStatus, PersonnelAttributeNames, SnopConfigurationKeys, PlanningFrequencies, SalesHistoryAttributes, DataScienceForecastStatus, DataScienceGetForecastAttributes, AbbreviatedForecastType, FileType, AlertType, AlertConfigurationKeys
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
import pyarrow as pa
# import polars as pd
import snappy
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta, date
from com_scai_dpai.helper.login import Login
from django.conf import settings
from django.http import HttpResponse
from com_scai_dpai import settings
from io import BytesIO as IO
import json
import re
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
from app.model.forecast.forecast_number import ForecastNumber
from django.db import transaction
from app.model.common.file import File
import urllib.request
import ujson
from threading import Thread
import concurrent.futures
import multiprocessing as mp
import gzip
from requests import get
import base64
import msgspec
from django.core.cache import cache
import threading
logger = logging.getLogger(__name__)
from app.model.forecast.alert import Alert
from app.serializers.forecast.alert import AlertSerializer
from app.crons.fileManager import CreateMasterFilesThread, FileManager
import urllib.request
from urllib.error import HTTPError
from app.helper.utils.NumpyArrayEncoder import NumpyArrayEncoder
import blosc
from app.services.forecast.memory import get_memory_usage
from app.utils import Util
from django.core.exceptions import ObjectDoesNotExist
import tracemalloc
from app.helper.alert.modifiedByManager import ModifiedByManagerAlertHelper
from app.helper.alert.snopForecastComparison import SnopForecastComparisonAlertHelper

class ForecastService():
    @transaction.atomic
    def save(self, tenant_id, bu_id, snop_id, data, token, forecast_type):
        try:
            logger.info(f"FORECAST-SAVE START {tenant_id}: {datetime.now()}")
            configurations = Configuration.get(tenant_id, bu_id, token)
            if ((forecast_type.upper() == ForecastType(2).name or forecast_type.upper() == ForecastType(4).name) and configurations[ForecastConfigurationKeys(2).name][ForecastConfigurationKeys(5).name]) or forecast_type.upper() == ForecastType(3).name:
                snop = Snop.objects.get(snop_id=snop_id, bu_id=bu_id, is_active=True)
                if data and snop: #and Util.isSnopActive(self, snop):
                    del snop
                    userEmail = base_util.getLoggedInUserEmailAddress(self, token)
                    if(forecast_type.upper() == ForecastType(2).name or forecast_type.upper() == ForecastType(4).name):
                        forecastUserHierarchyNoOfLevels = int(configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(1).name][EntitiesConfigurationKeys(2).name])
                        personelUserHierarchyKey = "ed"
                        alertKey = AlertConfigurationKeys(7).name
                        if forecast_type.upper() == ForecastType(4).name:
                            alertKey = AlertConfigurationKeys(9).name
                    else:
                        forecastUserHierarchyNoOfLevels = int(configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(7).name][EntitiesConfigurationKeys(8).name])
                        personelUserHierarchyKey = "es"
                        alertKey = AlertConfigurationKeys(9).name

                    if ("forecastDetails_" + forecast_type.upper() + "_" + snop_id) in cache: # and ("forecastApprovals_" + forecast_type.upper() + "_" + snop_id) in cache:
                        requestDataFrame = [
                            {
                                **detail,
                                "forecast_header_id": item["forecast_header_id"],
                                "remark_code": item["remark_code"],
                                "remark_description": item["remark_description"],
                            }
                            for item in data
                            for detail in item["forecast_detail"]
                        ]

                        requestDataFrame = pd.DataFrame(requestDataFrame)
                        # requestDataFrame = pd.json_normalize(data, "forecast_detail", ["forecast_header_id", "remark_code", "remark_description"])
                        requestHeaderIds = requestDataFrame['forecast_header_id']
                        # forecastDetailDataFrame = pa.deserialize_pandas(snappy.decompress(cache.get("forecastDetails_" + forecast_type.upper() + "_" + snop_id)))
                        dctx = zstd.ZstdDecompressor()
                        forecastDetailDataFrame = pa.deserialize_pandas(dctx.decompress(cache.get("forecastDetails_" + forecast_type.upper() + "_" + snop_id)))
                        forecastDetailDataFrame = forecastDetailDataFrame[forecastDetailDataFrame.fhi.isin(requestHeaderIds)]
                        # forecastApprovals = cache.get("forecastApprovals_" + forecast_type.upper() + "_" + snop_id)
                        # forecastApprovals = pd.DataFrame(forecastApprovals)
                        # forecastApprovals = forecastApprovals.astype({'forecast_header_id_fk_id':str})
                        # forecastApprovals = forecastApprovals[forecastApprovals.forecast_header_id_fk_id.isin(requestHeaderIds)]
                        del requestHeaderIds
                        if not forecastDetailDataFrame.empty: #and not forecastApprovals.empty:
                            def userLevel(row):
                                user_level=0
                                for index in range(forecastUserHierarchyNoOfLevels, 0, -1):
                                    if row[personelUserHierarchyKey + str(index)].upper() == userEmail.upper():
                                        user_level = index
                                        break
                                return user_level

                            forecastDetailDataFrame["ul"] = forecastDetailDataFrame.apply(userLevel, axis=1)
                            # user_level = 0
                            # forecastDetailDataFrameDemo = forecastDetailDataFrame.applymap(lambda x: str(x).upper() if x is not None else None)
                            # mask = (forecastDetailDataFrameDemo.loc[:, [personelUserHierarchyKey + str(index) for index in range(1, forecastUserHierarchyNoOfLevels + 1)]] == userEmail.upper()).any(axis=1)
                            # last_occurrence_index = mask[::-1].idxmax()
                            # last_row_level = forecastDetailDataFrameDemo.loc[last_occurrence_index]
                            # for index, value in last_row_level.items():
                            #     if isinstance(value, str) and value == userEmail.upper():
                            #         val = re.search(r'(\d+)$', index)
                            #         user_level = max(int(val.group()), user_level)
                            # forecastDetailDataFrame['ul'] = user_level
                            forecastDetailDataFrame = forecastDetailDataFrame[(forecastDetailDataFrame.ul > 0) & (forecastDetailDataFrame[(forecast_type.lower() + '_approved_till_level')] < forecastDetailDataFrame.ul)]
                            # forecastApprovals.set_index(['forecast_header_id_fk_id'], inplace=True)
                            # forecastDetailDataFrame.set_index(['fhi'], inplace=True,drop=False)
                            # forecastDetailDataFrame = forecastDetailDataFrame.join(forecastApprovals, how='inner')
                            # del forecastApprovals
                            # forecastDetailDataFrame = forecastDetailDataFrame[forecastDetailDataFrame.atl < forecastDetailDataFrame.ul]
                            forecastDetailDataFrame = forecastDetailDataFrame[['fhi', 'up', 'ul', 'sn', 'cn']]
                            forecastDetailDataFrame.reset_index(drop=True, inplace=True)
                            requestDataFrame = pd.merge(requestDataFrame, forecastDetailDataFrame, how="inner", left_on=["forecast_header_id"], right_on=["fhi"])
                            del forecastDetailDataFrame
                            requestDataFrame["value"] = requestDataFrame["up"]*requestDataFrame["volume"]
                            result = []
                            batch_size = 100000
                            num_rows = len(requestDataFrame)

                            for start in range(0, num_rows, batch_size):
                                end = min(start + batch_size, num_rows)
                                batch_rows = requestDataFrame.iloc[start:end]

                                for row in batch_rows.itertuples(index=False):
                                    result.append(dict(row._asdict()))
                            requestDataFrame = result

                            del result
                            response = []
                            remarks = []
                            remarksForecastHeaderIds = {}
                            alertsForecastHeaderIds = {}
                            alerts = []
                            createdForecast = []
                            deleteForecast = []
                            userId = base_util.getLoggedInUserId(token)

                            batch_size = 20000
                            requestDataFrameLen = len(requestDataFrame)
                            logger.info(f"Total record to process {tenant_id} {requestDataFrameLen}")
                            for start in range(0, requestDataFrameLen, batch_size):
                                batch = requestDataFrame[start:start+batch_size]
                                for data in batch:
                                    response.append({"vo": data["volume"],
                                        "v": data["value"], 
                                        "fdi": data["id"], 
                                        "fhi": data["fhi"]})
                                    
                                    createdForecast.append({"updated_by" : userId,
                                        "created_by" : userId,
                                        "volume" : round(data["volume"], 0), 
                                        "value" : float(data["value"]),
                                        "period" : data["period"],
                                        "forecast_header_id_fk_id" : data['fhi'],
                                        "forecast_detail_id" : data['id'],
                                        "forecast_type" : forecast_type.upper()})
                                
                                    deleteForecast.append(data['fhi'])

                                    if data["ul"] > 1 and not data["fhi"] in alertsForecastHeaderIds:
                                        alertsForecastHeaderIds[data["fhi"]] = "1"
                                        alerts.append({"fhi" : data["fhi"], "ul" : data["ul"]})

                                    if not data["fhi"] in remarksForecastHeaderIds:
                                        remarksForecastHeaderIds[data["fhi"]] = "1"
                                        remarks.append({
                                            "forecast_header_id" : data["fhi"],
                                            "remark_description" : data["remark_description"],
                                            "remark_code" : data["remark_code"],
                                            "updated_by" : userId
                                        })

                                # Delete ForecastDetail id 
                                ForecastDetail.objects.filter(Q(forecast_header_id_fk_id__in=deleteForecast) & Q(forecast_type=forecast_type.upper())).delete()
                                logger.info(f"delete items in a batch tenant_id {tenant_id} {len(deleteForecast)}")
                                deleteForecast = []

                                createdForecast = [ForecastDetail(**data_) for data_ in createdForecast]
                                # Create Forecast Detail with same deleted forecast id
                                ForecastDetail.objects.bulk_create(createdForecast, batch_size=batch_size, ignore_conflicts=True)
                                logger.info(f"created items in a batch tenant_id {tenant_id} {len(createdForecast)}")
                                createdForecast = []

                            if alerts:
                                # threading.Thread(target=ModifiedByManagerAlertHelper.generate, args=(tenant_id, bu_id, snop_id, configurations, alerts, forecast_type)).start()
                                alerts = []
                            del remarksForecastHeaderIds, alertsForecastHeaderIds, alerts, createdForecast, deleteForecast, requestDataFrame

                            if remarks:
                                batch_size = 100
                                for i in range(0, len(remarks), batch_size):
                                    batch = remarks[i:i + batch_size]
                                    batch = [ForecastHeader(**data) for data in batch]
                                    ForecastHeader.objects.bulk_update(batch, fields =  ['remark_code', 'remark_description', 'updated_by'])
                            del remarks
                            
                            threading.Thread(target=ForecastCache.updateDetails, args=(forecast_type, configurations, snop_id, tenant_id)).start()
                            logger.info(f"FORECAST-SAVE END {tenant_id}: {datetime.now()}")
                            if configurations[AlertConfigurationKeys(1).name][AlertConfigurationKeys(2).name][alertKey]:
                                threading.Thread(target=SnopForecastComparisonAlertHelper.generate, args=(tenant_id, bu_id, snop_id, configurations, forecast_type)).start()
                            return {"responseCode":ResponseCodes(7).name,"responseMessage": "Forecast Updated Successfully", "status": status.HTTP_200_OK, "data": response}
                        else:
                            logger.error(f"DPAI Service: {forecast_type}ForecastService-SAVE: Data Validation Failed: tenant_id: {tenant_id} | bu_id: {bu_id} | snop_id: {snop_id} | request body: {data}")
                            return {"responseCode":ResponseCodes(5).name,"responseMessage": "Invalid Request Body", "status": status.HTTP_400_BAD_REQUEST}
                    else:
                        logger.error(f"DPAI Service: {forecast_type}ForecastService-SAVE: Data Validation Failed: tenant_id: {tenant_id} | bu_id: {bu_id} | snop_id: {snop_id} | request body: {data}")
                        return {"responseCode":ResponseCodes(5).name,"responseMessage": "Invalid Request Body", "status": status.HTTP_400_BAD_REQUEST}
                else:
                    logger.error(f"DPAI Service: {forecast_type}ForecastService-SAVE: Invalid Request Body: tenant_id: {tenant_id} | bu_id: {bu_id} | snop_id: {snop_id}")
                    return {"responseCode":ResponseCodes(6).name,"responseMessage": "Invalid Request Body", "status": status.HTTP_400_BAD_REQUEST}
            else:
                logger.error(f"DPAI Service: {forecast_type}ForecastService-SAVE: Forecast is not enabled: tenant_id: {tenant_id} | bu_id: {bu_id} | snop_id: {snop_id}")
                return {"responseCode":ResponseCodes(3).name,"responseMessage": "Forecasting is not enabled", "status": status.HTTP_400_BAD_REQUEST}
        except Exception as e:
            logger.error(f"DPAI Service: {forecast_type}ForecastService-SAVE: Forecast save updated rejected with error: {e}")
            return {"responseCode":ResponseCodes(5).name,"responseMessage": "Forecast save rejected!", "status": status.HTTP_400_BAD_REQUEST}

    def get(self, tenant_id, bu_id, snop_id, token, forecast_type):
        try:
            logger.info(f"FORECAST-GET API START: {datetime.now()}")
            configurations = Configuration.get(tenant_id, bu_id, token)
            logger.info(f"FORECAST-GET CONFIGURATION GENERATED: {datetime.now()}")
            if ((forecast_type.upper() == ForecastType(2).name or forecast_type.upper() == ForecastType(4).name) and configurations[ForecastConfigurationKeys(2).name][ForecastConfigurationKeys(5).name]) or forecast_type.upper() == ForecastType(3).name:
                snop = Snop.objects.get(snop_id=snop_id, bu_id=bu_id, is_active=True)
                logger.info(f"FORECAST-GET SNOP GET SUCCESSFULL: {datetime.now()}")
                if snop:
                    forecast_result = []
                    sales_history_result = []
                    product_filter_result = {}
                    location_filter_result = {}
                    
                    forecastDataFrame = []
                    requiredColumns = []
                    planning_frequency = configurations[SnopConfigurationKeys(1).name][SnopConfigurationKeys(3).name]
                    userEmail = base_util.getLoggedInUserEmailAddress(self, token)
                    previousForecastUserNoOfLevels = 0
                    if(forecast_type.upper() == ForecastType(2).name or forecast_type.upper() == ForecastType(4).name):
                        forecastUserHierarchyNoOfLevels = int(configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(1).name][EntitiesConfigurationKeys(2).name])
                        personelUserHierarchyKey = "ed"
                        if forecast_type.upper() == ForecastType(4).name:
                            previousForecastUserNoOfLevels = configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(7).name][EntitiesConfigurationKeys(8).name]
                    else:
                        forecastUserHierarchyNoOfLevels = int(configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(7).name][EntitiesConfigurationKeys(8).name])
                        personelUserHierarchyKey = "es"
                        previousForecastUserNoOfLevels = configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(1).name][EntitiesConfigurationKeys(2).name]
                    logger.info(f"FORECAST-CREATE FORECAST RESPONSE START: {datetime.now()}")
                    userId = base_util.getLoggedInUserId(token)
                    result = ForecastService.createForecastResponse(self, forecastUserHierarchyNoOfLevels, configurations, forecast_type.upper(), previousForecastUserNoOfLevels, personelUserHierarchyKey, userEmail, snop_id, False)
                    return {'data': {"de": result}, 'responseCode': ResponseCodes(12).name, 'responseMessage': "Successfull!", "status": status.HTTP_200_OK}
                    logger.info(f"FORECAST-CREATE FORECAST RESPONSE COMPLETE: {datetime.now()}")    
                else:
                    logger.error(f"DPAI Service: {forecast_type}ForecastService-GET: Forecast is not enabled: tenant_id: {tenant_id} | bu_id: {bu_id} | snop_id: {snop_id}")
                    return {"responseCode":ResponseCodes(6).name,"responseMessage": "Invalid Request Body", "status": status.HTTP_400_BAD_REQUEST}
            else:
                logger.error(f"DPAI Service: {forecast_type}ForecastService-GET: Invalid Request: tenant_id: {tenant_id} | bu_id: {bu_id} | snop_id: {snop_id}")
                return {"responseCode":ResponseCodes(5).name,"responseMessage": "Invalid Request", "status": status.HTTP_400_BAD_REQUEST}
        except Exception as e:
            logger.error(f"DPAI Service: {forecast_type}ForecastService-GET: Forecast not available: {e}")
            return {"responseCode":ResponseCodes(10).name,"responseMessage": "Forecast not available!", "status": status.HTTP_400_BAD_REQUEST}

    def getFilter(self, tenant_id, bu_id, snop_id, token):
        try:
            snop = Snop.objects.get(snop_id=snop_id, bu_id=bu_id, is_active=True)
            files = File.objects.filter(file_type__in=[FileType(6).name], snop_id_fk=snop, is_active=True).values('file_type', 'file_name')
            filter_file_path = ""
            filter_result = {}
            if files:
                for fl in files:
                    if fl["file_type"] == FileType(6).name:
                        filter_file_path = fl["file_name"]
                        break
            if filter_file_path:
                with urllib.request.urlopen(filter_file_path) as filterFileUrl:
                    filter_result = msgspec.json.decode(filterFileUrl.read())
            return {"responseCode":ResponseCodes(40).name,"responseMessage": "Successfull!", "status": status.HTTP_200_OK, "data": {"f": filter_result}}
        except Exception as e:
            logger.error(f"DPAI Service: ForecastService-GET FILTER: Forecast Filter not available: {e}")
            return {"responseCode":ResponseCodes(10).name,"responseMessage": "Forecast Filter not available!", "status": status.HTTP_400_BAD_REQUEST}

    def getNetwork(self, tenant_id, bu_id, snop_id, token):
        try:
            snop = Snop.objects.get(snop_id=snop_id, bu_id=bu_id, is_active=True)
            files = File.objects.filter(file_type__in=[FileType(5).name], snop_id_fk=snop, is_active=True).values('file_type', 'file_name')
            network_file_path = ""
            network_result = []
            if files:
                for fl in files:
                    if fl["file_type"] == FileType(5).name:
                        network_file_path = fl["file_name"]
                        break
                
            if network_file_path:
                networkDataFrame = pd.read_csv(network_file_path)
                network_result = json.loads(networkDataFrame.to_json(orient='records',default_handler=str,date_format='iso'))
            return {"responseCode":ResponseCodes(41).name,"responseMessage": "Successfull!", "status": status.HTTP_200_OK, "data": {"nk": network_result}}
        except Exception as e:
            logger.error(f"DPAI Service: ForecastService-GET NETWORK: Forecast Network not available: {e}")
            return {"responseCode":ResponseCodes(10).name,"responseMessage": "Forecast Filter not available!", "status": status.HTTP_400_BAD_REQUEST}

    def getCSV(self, tenant_id, bu_id, snop_id, token, forecast_type):
        try:
            result = []
            forecastDetailDataFrame = []
            configurations = Configuration.get(tenant_id, bu_id, token)
            userEmail = base_util.getLoggedInUserEmailAddress(self, token)
            if ((forecast_type.upper() == ForecastType(2).name or forecast_type.upper() == ForecastType(4).name) and configurations[ForecastConfigurationKeys(2).name][ForecastConfigurationKeys(5).name]) or forecast_type.upper() == ForecastType(3).name:
                snop = Snop.objects.get(snop_id=snop_id, bu_id=bu_id, is_active=True)
                logger.info(f"FORECAST-GET SNOP GET SUCCESSFULL: {datetime.now()}")
                if snop:
                    csvFilePath = Util.GetBlobFilePath(f"Forecast_{forecast_type}_{snop_id}.csv")
                    forecastDetailDataFrame = pd.read_csv(csvFilePath, dtype={'Material Code': str})

            if(forecast_type.upper() == ForecastType(2).name or forecast_type.upper() == ForecastType(4).name):
                forecastUserHierarchyNoOfLevels = int(configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(1).name][EntitiesConfigurationKeys(2).name])
                personelUserHierarchyKey = "ed"
                if forecast_type.upper() == ForecastType(4).name:
                    previousForecastUserNoOfLevels = configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(7).name][EntitiesConfigurationKeys(8).name]
            else:
                forecastUserHierarchyNoOfLevels = int(configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(7).name][EntitiesConfigurationKeys(8).name])
                personelUserHierarchyKey = "es"
                previousForecastUserNoOfLevels = configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(1).name][EntitiesConfigurationKeys(2).name]

            def userLevel(row):
                user_level=0
                for index in range(forecastUserHierarchyNoOfLevels, 0, -1):
                    if row[personelUserHierarchyKey + str(index)].upper() == userEmail.upper():
                        user_level = index
                        break
                return user_level

            forecastDetailDataFrame["ul"] = forecastDetailDataFrame.apply(userLevel, axis=1)
            if forecast_type.upper() != ForecastType(2).name:
                previousForecastType = ForecastType(Util.castForecastType(forecast_type)-1).name.upper()
                forecastDetailDataFrame = forecastDetailDataFrame[(forecastDetailDataFrame.ul > 0) & (forecastDetailDataFrame[previousForecastType.lower() + "_approved_till_level"] == previousForecastUserNoOfLevels)]
            else:
                forecastDetailDataFrame = forecastDetailDataFrame[forecastDetailDataFrame.ul > 0]
            
            # if ("forecastApprovals_" + forecast_type.upper() + "_" + snop_id) in cache:
            #     forecastApprovals = cache.get("forecastApprovals_" + forecast_type.upper() + "_" + snop_id)
            #     forecastApprovals = pd.DataFrame(forecastApprovals)
            # else:
            #     forecastApprovals = ForecastCache.updateApprovals(forecast_type, configurations, snop_id, "")
            
            # if forecast_type.upper() != ForecastType(2).name:
            #     forecastApprovals = forecastApprovals[forecastApprovals[previousForecastType] == previousForecastUserNoOfLevels]
            
            # forecastApprovals = forecastApprovals.astype({'forecast_header_id_fk_id':str})
            # forecastApprovals.set_index(['forecast_header_id_fk_id'], inplace=True)
            # forecastDetailDataFrame.set_index(['fhi'], inplace=True, drop=False)
            # forecastDetailDataFrame = forecastDetailDataFrame.join(forecastApprovals, how='inner')
            
            dropColumns = ["fhi", "ul", "operational_approved_till_level", "sales_approved_till_level", "unconstrained_approved_till_level"]
            # if forecast_type.upper() == ForecastType(3).name:
            #     dropColumns.append(ForecastType(2).name)
            # elif forecast_type.upper() == ForecastType(4).name:
            #     dropColumns.append(ForecastType(3).name)
            if forecast_type.upper() != ForecastType(3).name:
                for index in range(1, int(configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(1).name][EntitiesConfigurationKeys(2).name])+1):
                    dropColumns.append("ed"+str(index))
            else:
                for index in range(1, int(configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(7).name][EntitiesConfigurationKeys(8).name])+1):
                    dropColumns.append("es"+str(index))
            
            forecastDetailDataFrame = forecastDetailDataFrame.drop(columns=dropColumns)

            """ download xlsx logic """
            excel_file = IO()
            xlwriter = pd.ExcelWriter(excel_file, engine='xlsxwriter')
            forecastDetailDataFrame.to_excel(xlwriter, 'sheet1', index=False)
            worksheet = xlwriter.sheets['sheet1']
            worksheet.set_column(0, 2, None, None, {'hidden': True})
            xlwriter.close()
            excel_file.seek(0)
            response = HttpResponse(excel_file.read(), content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            response['Content-Disposition'] = 'attachment; filename="data.xlsx"'
            return response
        except HTTPError as e:
            logger.info(f"DPAI Service: getXLSX: File does not exist!: {e}")
            return {"responseCode": ResponseCodes(10).name, "responseMessage": "File does not exist!",
                    "status": status.HTTP_404_NOT_FOUND}
        except Exception as e:
            logger.error(f"DPAI Service: getXLSX: get xlsx create with error: {e}")
            return {"responseCode": ResponseCodes(10).name, "responseMessage": {e},
                    "status": status.HTTP_400_BAD_REQUEST}

    @transaction.atomic
    def create(self, tenant_id, bu_id, data):
        try:
            if data:
                sysToken = Login.get_token(settings.SYS_ADMIN_EMAIL, settings.SYS_ADMIN_PASSWORD)
                forecast_number = data["id"]
                logger.info(f'CREATE FORECAST : {str(forecast_number)}')
                forecast = ForecastNumber.objects.get(forecast_number = forecast_number)
                if forecast and forecast.forecast_status != DataScienceForecastStatus(1).name.upper() and forecast.forecast_status != DataScienceForecastStatus(5).name.upper():
                    snop = Snop.objects.get(snop_id = forecast.snop_id)
                    if snop:
                        configuration_response = Configuration.get(tenant_id, snop.bu_id, sysToken)
                        logger.info(f'CREATE FORECAST Trying to get Forecast for {forecast.forecast_number}')
                        # API Integration
                        if configuration_response and data[DataScienceGetForecastAttributes(1).name] and data[DataScienceGetForecastAttributes(1).name].upper() == DataScienceForecastStatus(1).name.upper():
                            forecast_csv_link = data[DataScienceGetForecastAttributes(2).name][DataScienceGetForecastAttributes(3).name]
                            forecastDataFrame = pd.read_csv(forecast_csv_link)
                            forecastUniqueDataFrame = forecastDataFrame[[DataScienceGetForecastAttributes(15).name, DataScienceGetForecastAttributes(16).name, DataScienceGetForecastAttributes(17).name]].drop_duplicates()
                            detail_values = []
                            # approval_values = []
                            header_values = []
                            systemUserId = base_util.getLoggedInUserId(sysToken)
                            currentDate = timezone.now()
                            ForecastHeader.objects.filter(is_active = True, is_re_forecasted = True, snop_id_fk = snop).update(is_active = False)
                            
                            forecastDataFrame = json.loads(forecastDataFrame.to_json(orient='records',default_handler=str,date_format='iso'))
                            forecast_classification_csv_link = data[DataScienceGetForecastAttributes(2).name][DataScienceGetForecastAttributes(18).name]
                            forecastClassificationDataFrame = pd.read_csv(forecast_classification_csv_link)
                            forecastUniqueDataFrame = pd.merge(forecastUniqueDataFrame, forecastClassificationDataFrame, how="left", left_on=[DataScienceGetForecastAttributes(15).name, DataScienceGetForecastAttributes(16).name, DataScienceGetForecastAttributes(17).name], right_on=[DataScienceGetForecastAttributes(15).name, DataScienceGetForecastAttributes(16).name, DataScienceGetForecastAttributes(17).name])
                            forecast_profile_bag_csv_link = data[DataScienceGetForecastAttributes(2).name][DataScienceGetForecastAttributes(20).name]
                            forecastProfileBagDataFrame = pd.read_csv(forecast_profile_bag_csv_link)
                            forecastUniqueDataFrame = pd.merge(forecastUniqueDataFrame, forecastProfileBagDataFrame, how="left", left_on=[DataScienceGetForecastAttributes(15).name, DataScienceGetForecastAttributes(16).name, DataScienceGetForecastAttributes(17).name], right_on=[DataScienceGetForecastAttributes(15).name, DataScienceGetForecastAttributes(16).name, DataScienceGetForecastAttributes(17).name])
                            forecastUniqueDataFrame = json.loads(forecastUniqueDataFrame.to_json(orient='records',default_handler=str,date_format='iso'))
                            for key in forecastUniqueDataFrame:
                                sparsity = "NA"
                                if key[DataScienceGetForecastAttributes(21).name]:
                                    if key[DataScienceGetForecastAttributes(21).name].upper() == "HIGH":
                                        sparsity = "H"
                                    elif key[DataScienceGetForecastAttributes(21).name].upper() == "MEDIUM":
                                        sparsity = "M"
                                    elif key[DataScienceGetForecastAttributes(21).name].upper() == "LOW":
                                        sparsity = "L"
                                header_values.append(ForecastHeader(
                                    snop_id_fk=snop,
                                    sku_id=key[DataScienceGetForecastAttributes(15).name],
                                    node_id=key[DataScienceGetForecastAttributes(16).name],
                                    channel_id=key[DataScienceGetForecastAttributes(17).name],
                                    variability=key[DataScienceGetForecastAttributes(23).name] if key[DataScienceGetForecastAttributes(23).name] else "NA",
                                    segment=key[DataScienceGetForecastAttributes(19).name],
                                    adi=1.32,#TODO
                                    cv=round(float(key[DataScienceGetForecastAttributes(24).name]),2),
                                    created_at=currentDate,
                                    updated_at=currentDate,
                                    created_by=systemUserId,
                                    updated_by=systemUserId,
                                    sparsity=sparsity,
                                    is_seasonal = key[DataScienceGetForecastAttributes(22).name],
                                    fmr=key[DataScienceGetForecastAttributes(26).name] if DataScienceGetForecastAttributes(26).name in key else "NA",
                                    operational_approved_till_level = int(configuration_response[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(1).name][EntitiesConfigurationKeys(2).name]) if not configuration_response[ForecastConfigurationKeys(2).name][ForecastConfigurationKeys(5).name] else 0,
                                    sales_approved_till_level = 0,
                                    unconstrained_approved_till_level = 0
                                ))

                                # '''Add data to ForecastApproval'''
                                # for f_type in [ForecastType(4).name.upper(), ForecastType(2).name.upper(), ForecastType(3).name.upper()]:
                                #     approval_values.append({
                                #         "sku_id" : key[DataScienceGetForecastAttributes(15).name],
                                #         "node_id" : key[DataScienceGetForecastAttributes(16).name],
                                #         "channel_id" : key[DataScienceGetForecastAttributes(17).name],
                                #         "approved_till_level": 0,
                                #         "created_at" : currentDate,
                                #         "updated_at" : currentDate,
                                #         "created_by" : systemUserId,
                                #         "updated_by" : systemUserId,
                                #         "forecast_type": f_type
                                #     })

                            logger.info('CREATE FORECAST: Forecast Header & Approval created successfully')

                            for row in forecastDataFrame:
                                '''Add data to ForecastDetail'''
                                for forecast_type in [ForecastType(1).name.upper(), ForecastType(2).name.upper()]:
                                    detail_values.append({
                                        "sku_id" : row[DataScienceGetForecastAttributes(15).name],
                                        "node_id" : row[DataScienceGetForecastAttributes(16).name],
                                        "channel_id" : row[DataScienceGetForecastAttributes(17).name],
                                        "volume":round(row[DataScienceGetForecastAttributes(5).name],0),
                                        "value":float(row[DataScienceGetForecastAttributes(14).name] if row[DataScienceGetForecastAttributes(14).name] else 0),
                                        "created_at" : currentDate,
                                        "updated_at" : currentDate,
                                        "created_by" : systemUserId,
                                        "updated_by" : systemUserId,
                                        "forecast_type": forecast_type,
                                        "period": parse(row[DataScienceGetForecastAttributes(13).name]).date()
                                    })
                                if not configuration_response[ForecastConfigurationKeys(2).name][ForecastConfigurationKeys(5).name]:  
                                    detail_values.append({
                                        "sku_id" : row[DataScienceGetForecastAttributes(15).name],
                                        "node_id" : row[DataScienceGetForecastAttributes(16).name],
                                        "channel_id" : row[DataScienceGetForecastAttributes(17).name],
                                        "volume":round(row[DataScienceGetForecastAttributes(5).name],0),
                                        "value":float(row[DataScienceGetForecastAttributes(14).name] if row[DataScienceGetForecastAttributes(14).name] else 0),
                                        "created_at" : currentDate,
                                        "updated_at" : currentDate,
                                        "created_by" : systemUserId,
                                        "updated_by" : systemUserId,
                                        "forecast_type": ForecastType(3).name.upper(),
                                        "period": parse(row[DataScienceGetForecastAttributes(13).name]).date()
                                    })
                            logger.info('CREATE FORECAST: Forecast Details created successfully')
                                
                            '''Add data to ForecastHeader'''
                            ForecastHeader.objects.bulk_create(header_values, 20000, ignore_conflicts=True)
                            logger.info('CREATE FORECAST ForecastHeader data added successfully')

                            updatedForecastHeaders = ForecastHeader.objects.filter(is_active = True, is_re_forecasted = False, snop_id_fk = snop).values('sku_id', 'node_id', 'channel_id', 'forecast_header_id')
                            forecastDetailsDataFrame = pd.DataFrame(detail_values)
                            # forecastApprovalsDataFrame = pd.DataFrame(approval_values)
                            forecastHeadersDataFrame = pd.DataFrame.from_records(updatedForecastHeaders)
                            forecastHeadersDataFrame = forecastHeadersDataFrame.astype({'sku_id':str,'node_id':str,'channel_id':str})
                            forecastHeadersDataFrame = forecastHeadersDataFrame.rename(columns={"forecast_header_id":"forecast_header_id_fk"})

                            forecastDetailsDataFrame = pd.merge(forecastDetailsDataFrame, forecastHeadersDataFrame, how="inner", left_on=["sku_id", "node_id", "channel_id"], right_on=["sku_id", "node_id", "channel_id"])
                            forecastDetailsDataFrame = forecastDetailsDataFrame[["volume", "value", "created_at", "updated_at", "created_by", "updated_by", "forecast_type", "period", "forecast_header_id_fk"]]
                            forecastDetailsDataFrame = json.loads(forecastDetailsDataFrame.to_json(orient='records',default_handler=str,date_format='iso'))
                            updatedForecastDetails = []
                            for detail in forecastDetailsDataFrame:
                                updatedForecastDetails.append(ForecastDetail(
                                        volume=round(detail["volume"],0),
                                        value=float(detail["value"]),
                                        created_at=currentDate,
                                        updated_at=currentDate,
                                        created_by=systemUserId,
                                        updated_by=systemUserId,
                                        forecast_header_id_fk_id=detail["forecast_header_id_fk"],
                                        forecast_type=detail["forecast_type"],
                                        period=parse(detail["period"]).date()
                                    ))
                            ForecastDetail.objects.bulk_create(updatedForecastDetails, 20000, ignore_conflicts=True)
                            logger.info('CREATE FORECAST ForecastDetail data added successfully')
                            
                            # forecastApprovalsDataFrame = pd.merge(forecastApprovalsDataFrame, forecastHeadersDataFrame, how="inner", left_on=["sku_id", "node_id", "channel_id"], right_on=["sku_id", "node_id", "channel_id"])
                            # forecastApprovalsDataFrame = forecastApprovalsDataFrame[["created_at", "updated_at", "created_by", "updated_by", "forecast_type", "approved_till_level", "forecast_header_id_fk"]]
                            # forecastApprovalsDataFrame = json.loads(forecastApprovalsDataFrame.to_json(orient='records',default_handler=str,date_format='iso'))
                            # updatedForecastApprovals = []
                            # for approval in forecastApprovalsDataFrame:
                            #     updatedForecastApprovals.append(ForecastApproval(
                            #             approved_till_level=approval["approved_till_level"],
                            #             created_at=currentDate,
                            #             updated_at=currentDate,
                            #             created_by=systemUserId,
                            #             updated_by=systemUserId,
                            #             forecast_header_id_fk_id=approval["forecast_header_id_fk"],
                            #             forecast_type=approval["forecast_type"]
                            #         ))
                            # ForecastApproval.objects.bulk_create(updatedForecastApprovals, 20000, ignore_conflicts=True)
                            # logger.info('CREATE FORECAST ForecastApproval data added successfully')
                            
                            forecast.forecast_status = DataScienceForecastStatus(1).name.upper()
                            forecast.save()
                            threading.Thread(target=CreateMasterFilesThread, args=(tenant_id, snop, configuration_response)).start()
                            return {"responseCode":ResponseCodes(30).name,"responseMessage": "Forecast created successfully", "status": status.HTTP_200_OK, "data": {}}
                        else:
                            if data[DataScienceGetForecastAttributes(1).name] and data[DataScienceGetForecastAttributes(1).name].upper() == DataScienceForecastStatus(5).name.upper():
                                forecast.forecast_status = DataScienceForecastStatus(5).name.upper()
                                forecast.save()
                                logger.info(f'DPAI Service: CREATE Forecast: Forecast failed status updated' + f'{forecast.forecast_number}')
                                return {"responseCode":ResponseCodes(30).name,"responseMessage": "Forecast failed status updated", "status": status.HTTP_200_OK}
                            else:
                                logger.info(f'DPAI Service: CREATE Forecast: Forecast not yet ready for ' + f'{forecast.forecast_number}')
                                return {"responseCode":ResponseCodes(30).name,"responseMessage": "Forecast not yet ready", "status": status.HTTP_200_OK}
                    else:
                        logger.error(f"DPAI Service: ForecastService-create: Invalid SNOP")
                        return {"responseCode":ResponseCodes(5).name,"responseMessage": "Invalid Request", "status": status.HTTP_400_BAD_REQUEST}
                else:
                    logger.error(f"DPAI Service: ForecastService-create: Invalid Forecast Status")
                    return {"responseCode":ResponseCodes(28).name,"responseMessage": "Forecast Already Received", "status": status.HTTP_400_BAD_REQUEST}
            else:
                logger.error(f"DPAI Service: ForecastService-create: Invalid Request body")
                return {"responseCode":ResponseCodes(5).name,"responseMessage": "Invalid Request", "status": status.HTTP_400_BAD_REQUEST}
        except Exception as e:
            logger.error(f"DPAI Service: ForecastService-create: Forecast not created: {e}")
            return {"responseCode":ResponseCodes(23).name,"responseMessage": "Forecast not created!", "status": status.HTTP_400_BAD_REQUEST}

    def createForecastResponse(self, forecastUserHierarchyNoOfLevels, configurations, forecast_type, previousForecastUserNoOfLevels, personelUserHierarchyKey, userEmail, snop_id, isAnalytics):
        try:
            result = []
            forecast_details_result = []
            if ("forecastDetails_" + forecast_type.upper() + "_" + snop_id) in cache:
                logger.info(f"FORECAST-GET BEFORE FORECAST DETAILS FROM CACHE: {datetime.now()}")
                dctx = zstd.ZstdDecompressor()
                forecast_details_result = pa.deserialize_pandas(dctx.decompress(cache.get("forecastDetails_" + forecast_type.upper() + "_" + snop_id)))
                logger.info(f"FORECAST-GET BEFORE FORECAST DETAILS AFTER CACHE: {datetime.now()}")
                def convert_np_for_sh(item):
                    if isinstance(item, np.ndarray):
                        return item.tolist()
                    else:
                        return item

                def convert_dict_values_for_fd(item):
                    if isinstance(item, dict):
                        return {k: convert_value(v) for k, v in item.items()}
                    else:
                        return item

                def convert_value(value):
                    if isinstance(value, np.ndarray):
                        return value.tolist()
                    elif isinstance(value, list):
                        return [convert_value(v) for v in value]
                    elif isinstance(value, dict):
                        return {k: convert_value(v) for k, v in value.items()}
                    else:
                        return value
                
                logger.info(f"FORECAST-GET BEFORE RECURSIVE CONVERT: {datetime.now()}")
                forecast_details_result['sh'] = forecast_details_result['sh'].apply(convert_np_for_sh)
                forecast_details_result['fd'] = forecast_details_result['fd'].apply(convert_dict_values_for_fd)
                forecastDetailDataFrame = forecast_details_result
                logger.info(f"FORECAST-GET AFTER RECURSIVE CONVERT: {datetime.now()}")
            else:
                forecast_details_result = ForecastCache.updateDetails(forecast_type, configurations, snop_id, "")
                forecastDetailDataFrame = forecast_details_result

            if isAnalytics:
                salesUserHierarchyNoOfLevels = int(configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(7).name][EntitiesConfigurationKeys(8).name])
                salesUserHierarchyKey = "es"

            def userLevel(row):
                user_level=0
                for index in range(forecastUserHierarchyNoOfLevels, 0, -1):
                    if row[personelUserHierarchyKey + str(index)].upper() == userEmail.upper():
                        user_level = index
                        break
                if isAnalytics and user_level == 0:
                    for index in range(salesUserHierarchyNoOfLevels, 0, -1):
                        if row[salesUserHierarchyKey + str(index)].upper() == userEmail.upper():
                            user_level = index
                            break
                return user_level

            forecastDetailDataFrame["ul"] = forecastDetailDataFrame.apply(userLevel, axis=1)
            logger.info(f"FORECAST-GET CREATE FORECAST RESPONSE USER LEVEL FIRST: {datetime.now()}")
            if forecast_type.upper() != ForecastType(2).name:
                previousForecastType = ForecastType(Util.castForecastType(forecast_type)-1).name.upper()
                forecastDetailDataFrame = forecastDetailDataFrame[(forecastDetailDataFrame.ul > 0) & (forecastDetailDataFrame[previousForecastType.lower() + "_approved_till_level"] == previousForecastUserNoOfLevels)]
            else:
                forecastDetailDataFrame = forecastDetailDataFrame[forecastDetailDataFrame.ul > 0]
            
            # if ("forecastApprovals_" + forecast_type.upper() + "_" + snop_id) in cache:
            #     logger.info(f"FORECAST-GET BEFORE FORECAST APPROVALS FROM CACHE: {datetime.now()}")
            #     forecastApprovals = cache.get("forecastApprovals_" + forecast_type.upper() + "_" + snop_id)
            #     forecastApprovals = pd.DataFrame(forecastApprovals)
            #     logger.info(f"FORECAST-GET BEFORE FORECAST APPROVALS AFTER CACHE: {datetime.now()}")
            # else:
            #     forecastApprovals = ForecastCache.updateApprovals(forecast_type, configurations, snop_id, "")
            
            # if forecast_type.upper() != ForecastType(2).name:
            #     forecastApprovals = forecastApprovals[forecastApprovals[previousForecastType] == previousForecastUserNoOfLevels]
            
            # forecastApprovals = forecastApprovals.astype({'forecast_header_id_fk_id':str})
            # logger.info(f"FORECAST-GET FORECAST HEADER IDS: {datetime.now()}")
            # forecastApprovals.set_index(['forecast_header_id_fk_id'], inplace=True)
            # forecastDetailDataFrame.set_index(['fhi'], inplace=True,drop=False)
            # forecastDetailDataFrame = forecastDetailDataFrame.join(forecastApprovals, how='inner')
            logger.info(f"FORECAST-GET FORECAST DETAIL DATAFRAME FILTERED AS PER APPROVALS: {datetime.now()}")
            forecastTypeApprovalKey = forecast_type.lower() + '_approved_till_level'
            
            def st(row):
                status = np.where(
                    row[forecastTypeApprovalKey] == forecastUserHierarchyNoOfLevels,
                    ForecastStatus(3).name,
                    np.where(
                        row[forecastTypeApprovalKey] < row["ul"],
                        ForecastStatus(1).name,
                        ForecastStatus(2).name
                    )
                )
                return status

            forecastDetailDataFrame["st"] = st(forecastDetailDataFrame)
            forecastDetailDataFrame = forecastDetailDataFrame.rename(columns={forecastTypeApprovalKey:"atl"})
            logger.info(f"FORECAST-GET FORECAST STATUS: {datetime.now()}")
            
            # dropColumns = ['lstd', 'lsn', 'lsled']
            dropColumns = []
            if isAnalytics:
                dropColumns+= ['rc', 'rd', 'st', 'ul', 'atl', 'sales_approved_till_level', 'unconstrained_approved_till_level']
            else:
                for index in range(2, 5):
                    if forecast_type.upper() != ForecastType(index).name:
                        dropColumns.append(ForecastType(index).name.lower() + "_approved_till_level")
                dropColumns+= ['fmr', 'is', 'sp', 'ysvo', 'ysv', 'l3sv', 'l3svo', 'nfv', 'nfvo', 'nnfv', 'nnfvo']
            
            for index in range(1, int(configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(1).name][EntitiesConfigurationKeys(2).name])+1):
                dropColumns.append("ed"+str(index))

            forecastDetailDataFrame = forecastDetailDataFrame.drop(columns=dropColumns)
            logger.info(f"FORECAST-GET FORECAST DROP COLUMNS: {datetime.now()}")
            result = []  # Initialize an empty list to store the processed data
            batch_size = 100000  # Size of each batch of data to be processed
            num_rows = len(forecastDetailDataFrame)  # Get the total number of rows in the DataFrame

            # Iterate through the data in batches
            for start in range(0, num_rows, batch_size):
                end = min(start + batch_size, num_rows)  # Calculate the end index of the current batch
                batch_rows = forecastDetailDataFrame.iloc[start:end]  # Extract the current batch of rows

                # Iterate through each row in the batch
                for row in batch_rows.values:
                    row_dict = dict(zip(forecastDetailDataFrame.columns, row))
                    result.append(row_dict)  # Append the dictionary to the result list
            
            logger.info(f"FORECAST-GET FORECAST TO DICT: {datetime.now()}")
            return result
        except Exception as e:
            logger.error(f"DPAI Service: createForecastResponse: Forecast Data create with error: {e}")
            return {'responseCode': ResponseCodes(5).name, 'responseMessage': "Invalid Request", "status": status.HTTP_400_BAD_REQUEST}
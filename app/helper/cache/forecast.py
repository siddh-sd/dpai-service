import brotli
import zstandard as zstd
from app.helper.utils.ZstdReadWriter import ZstdReadWriter
from com_scai_dpai.utils import Util as base_util
from app.utils import Util
from com_scai_dpai.helper.configuration import Configuration
from app.enum import ForecastConfigurationKeys, EntitiesConfigurationKeys, ForecastType, ResponseCodes, NetworkAttributeNames, ProductAttributeNames, LocationAttributeNames, SegmentationType, ForecastStatus, PersonnelAttributeNames, SnopConfigurationKeys, PlanningFrequencies, SalesHistoryAttributes, DataScienceForecastStatus, DataScienceGetForecastAttributes, AbbreviatedForecastType, FileType
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
from django_tenants.utils import tenant_context, get_tenant_model
import snappy
from app.helper.utils.NumpyArrayEncoder import NumpyArrayEncoder
from django.http import HttpResponse
from collections import defaultdict

class ForecastCache():
    def updateDetails(forecast_type, configurations, snop_id, tenant_id):
        if tenant_id != "":
            with tenant_context(get_tenant_model().objects.get(tenant_id=tenant_id)):
                return ForecastCache.updateForecastDetails(forecast_type, configurations, snop_id)
        else:
            return ForecastCache.updateForecastDetails(forecast_type, configurations, snop_id)

    def updateForecastDetails(forecast_type, configurations, snop_id):
        logger.info(f"FORECASTCACHE-DETAIL START {snop_id}: {datetime.now()}")
        files = File.objects.filter(file_type__in=[FileType(3).name, FileType(5).name, FileType(6).name], snop_id_fk_id=snop_id, is_active=True).values('file_type', 'file_name')
        sales_history_forecast_mapping_file_path = ""
        if files:
            for fl in files:
                if fl["file_type"] == FileType(3).name:
                    sales_history_forecast_mapping_file_path = fl["file_name"]
                    break

        forecastDataFrame = []
        requiredColumns = []
        forecast_details_result = []

        if sales_history_forecast_mapping_file_path:
            sales_history_forecast_mapping_result = get(sales_history_forecast_mapping_file_path).content
            logger.info(f"FORECASTCACHE-DETAIL AFTER GET {snop_id}: {datetime.now()}")
            # sales_history_forecast_mapping_result = gzip.decompress(sales_history_forecast_mapping_result)
            sales_history_forecast_mapping_result = ZstdReadWriter.decompressZSDContent(sales_history_forecast_mapping_result)
            sales_history_forecast_mapping_result = msgspec.json.decode(sales_history_forecast_mapping_result)
            logger.info(f"FORECASTCACHE-DETAIL AFTER DECOMPRESS {snop_id}: {datetime.now()}")
            forecastDataFrame = pd.DataFrame(sales_history_forecast_mapping_result)
            forecastHeaderIds = forecastDataFrame['fhi']
            logger.info(f"FORECASTCACHE-DETAIL AFTER DATA FRAME {snop_id}: {datetime.now()}")
            forecastHeaders = list(ForecastHeader.objects.filter(forecast_header_id__in=forecastHeaderIds, is_active=True).values('forecast_header_id', "remark_code", 'remark_description', 'operational_approved_till_level', 'sales_approved_till_level', 'unconstrained_approved_till_level'))
            logger.info(f"FORECASTCACHE-DETAIL AFTER REMARKS GET FROM DATABASE {snop_id}: {datetime.now()}")
            forecastHeadersDataFrame = pd.DataFrame(forecastHeaders)
            forecastHeadersDataFrame = forecastHeadersDataFrame.astype({'forecast_header_id':str})
            previousForecastUserHierarchyNoOfLevels = ""
            
            if(forecast_type.upper() == ForecastType(2).name or forecast_type.upper() == ForecastType(4).name):
                personelUserHierarchyKey = "ed"
                forecastUserHierarchyNoOfLevels = int(configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(1).name][EntitiesConfigurationKeys(2).name])
                if forecast_type.upper() == ForecastType(4).name:
                    previousForecastUserHierarchyNoOfLevels = int(configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(7).name][EntitiesConfigurationKeys(8).name])
                    previousForecastType = ForecastType(3).name
            else:
                personelUserHierarchyKey = "es"
                forecastUserHierarchyNoOfLevels = int(configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(7).name][EntitiesConfigurationKeys(8).name])
                previousForecastUserHierarchyNoOfLevels = int(configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(1).name][EntitiesConfigurationKeys(2).name])
                previousForecastType = ForecastType(2).name
            
            if forecast_type.upper() != ForecastType(2).name:
                forecastHeadersDataFrame = forecastHeadersDataFrame[forecastHeadersDataFrame[previousForecastType.lower() + "_approved_till_level"] == previousForecastUserHierarchyNoOfLevels]
                forecastDataFrame = pd.merge(forecastDataFrame, forecastHeadersDataFrame, how="inner", left_on=["fhi"], right_on=["forecast_header_id"])
            else:
                forecastDataFrame = pd.merge(forecastDataFrame, forecastHeadersDataFrame, how="left", left_on=["fhi"], right_on=["forecast_header_id"])
            
            forecastHeaderIds = forecastDataFrame['fhi']
            forecastDataFrame = forecastDataFrame.drop(columns=['forecast_header_id'])
            forecastDataFrame = forecastDataFrame.rename(columns={"remark_code":"rc", "remark_description":"rd"})
            forecastDataFrame['rc'].fillna('', inplace=True)
            forecastDataFrame['rd'].fillna('', inplace=True)
            logger.info(f"FORECASTCACHE-DETAIL AFTER REMARKS OPERATIONS {snop_id}: {datetime.now()}")
            # if forecast_type.upper() != ForecastType(2).name:
            #     forecastHeaderIds = []
            #     previousForecastType = ForecastType(Util.castForecastType(forecast_type)-1).name
            #     forecastApprovals = ForecastApproval.objects.filter(forecast_header_id_fk_id__in=forecastDataFrame['fhi'], forecast_type__in=[previousForecastType], approved_till_level=previousForecastUserHierarchyNoOfLevels).values('forecast_header_id_fk_id')
            #     forecastApprovals = pd.DataFrame(forecastApprovals)
            #     if not forecastApprovals.empty:
            #         forecastHeaderIds = forecastApprovals['forecast_header_id_fk_id']
            forecast_detail_query = Q()
            for index in range(1, Util.castForecastType( forecast_type) + 1):
                forecast_detail_query |= Q(forecast_header_id_fk_id__in=forecastHeaderIds,
                        forecast_type__iexact=ForecastType(index).name, is_active=True)
            logger.info(f"FORECASTCACHE-DETAIL BEFORE GET FORECAST DETAILS {snop_id}: {datetime.now()}")
            forecast_details = list(ForecastDetail.objects.filter(forecast_detail_query).values('forecast_detail_id', 'period', 'volume', 'value', 'forecast_type', 'forecast_header_id_fk_id').order_by('forecast_header_id_fk_id', 'forecast_type', 'period'))
            logger.info(f"FORECASTCACHE-DETAIL AFTER GET FORECAST  DETAILS {snop_id}: {datetime.now()}")
            forecastTypeValue = Util.castForecastType(forecast_type)
            planningHorizon = int(configurations[SnopConfigurationKeys(1).name][SnopConfigurationKeys(2).name])
            threshold = float(configurations[ForecastConfigurationKeys(2).name][ForecastConfigurationKeys(15).name])
            numberOfRecordsForEachHeader = forecastTypeValue*planningHorizon
            numberOfRecordsForEachHeaderLessOne = numberOfRecordsForEachHeader - 1
            
            forecastDetailsLength = len(forecast_details)
            logger.info(f"FORECASTCACHE-DETAIL BEFORE FOR LOOP {snop_id}: {datetime.now()}")
            abbreviated_forecast_type_map = {
                        ForecastType(2).name: "o",
                        ForecastType(3).name: "sa",
                        ForecastType(1).name: "s",
                        ForecastType(4).name: "u",
                    }
            
            forecast_details_result = []
            forecastHeader = None
            forecastHeaderDict = {
                        4: {"s": {"fdi": [], "p": [], "vo": [], "v": []}, "o": {"fdi": [], "p": [], "vo": [], "v": []}, "sa": {"fdi": [], "p": [], "vo": [], "v": []}, "u": {"fdi": [], "p": [], "vo": [], "v": []}},
                        3: {"s": {"fdi": [], "p": [], "vo": [], "v": []}, "o": {"fdi": [], "p": [], "vo": [], "v": []}, "sa": {"fdi": [], "p": [], "vo": [], "v": []}},
                        2: {"s": {"fdi": [], "p": [], "vo": [], "v": []}, "o": {"fdi": [], "p": [], "vo": [], "v": []}}
                    }
            for index, detail in enumerate(forecast_details):
                forecast_type_name = detail["forecast_type"]
                abbreviated_forecast_type = abbreviated_forecast_type_map.get(forecast_type_name)
                if index % numberOfRecordsForEachHeader == 0:
                    forecastHeader = {
                        "fhi": str(detail["forecast_header_id_fk_id"]),
                        "fd": copy.deepcopy(forecastHeaderDict.get(forecastTypeValue))
                    }
                
                forecastHeader["fd"][abbreviated_forecast_type]["fdi"].extend([str(detail['forecast_detail_id'])])
                forecastHeader["fd"][abbreviated_forecast_type]["p"].extend([str(detail['period'])])
                forecastHeader["fd"][abbreviated_forecast_type]["vo"].extend([detail['volume']])
                forecastHeader["fd"][abbreviated_forecast_type]["v"].extend([float(detail['value'])])
                # for key, value in zip(("fdi", "p", "vo", "v"), (str(detail['forecast_detail_id']), str(detail['period']), detail['volume'], float(detail['value']))):
                #     forecastHeader["fd"][abbreviated_forecast_type][key].append(value)
                if index % numberOfRecordsForEachHeader == numberOfRecordsForEachHeader - 1:
                    forecastHeader['et'] = False
                    if threshold != 0:
                        for thresholdIndex in range(planningHorizon):
                            stat_volume = forecastHeader["fd"]["s"]["vo"][thresholdIndex]
                            forecast_type_volume = forecastHeader["fd"][abbreviated_forecast_type]["vo"][thresholdIndex]
                            if forecast_type_volume > (stat_volume + (threshold * stat_volume)) or forecast_type_volume < (stat_volume - (threshold * stat_volume)):
                                forecastHeader['et'] = True
                                break
                    forecast_details_result.append(forecastHeader)
            logger.info(f"FORECASTCACHE-DETAIL AFTER FOR LOOP {snop_id}: {datetime.now()}")
            if forecast_details_result:
                forecastDetailDataFrame = pd.DataFrame(forecast_details_result)
                forecastDataFrame = pd.merge(forecastDataFrame, forecastDetailDataFrame, how="inner", left_on=["fhi"], right_on=["fhi"])       
                # forecastDataFrame = forecastDataFrame.to_dict(orient='records')
                cctx = zstd.ZstdCompressor()
                cache.set(("forecastDetails_" + forecast_type.upper() + "_" + snop_id), cctx.compress(pa.serialize_pandas(forecastDataFrame).to_pybytes()))
                # cache.set(("forecastDetails_" + forecast_type.upper() + "_" + snop_id), brotli.compress(pa.serialize_pandas(forecastDataFrame).to_pybytes()))  
                # cache.set(("forecastDetails_" + forecast_type.upper() + "_" + snop_id), snappy.compress(pa.serialize_pandas(forecastDataFrame).to_pybytes()))
                ForecastCache.generateForecastCSV(forecastDataFrame, forecast_type, configurations, snop_id, forecastUserHierarchyNoOfLevels, personelUserHierarchyKey, "")
                logger.info(f"FORECASTCACHE-DETAIL END {snop_id}: {datetime.now()}")
                # forecastDetailsUserKeys = cache.keys("forecastDetails_" + forecast_type.upper() + "_" + snop_id + "_*")
                # if forecastDetailsUserKeys:
                #     for forecastDetailsUserKey in forecastDetailsUserKeys:
                #         user_id = forecastDetailsUserKey.replace("forecastDetails_" + forecast_type.upper() + "_" + snop_id + "_","")
                #         userEmail = cache.get(user_id)
                #         forecastDetailDataFrame = forecastDataFrame[forecastDataFrame.columns]
                #         def userLevel(row):
                #             user_level=0
                #             for index in range(forecastUserHierarchyNoOfLevels, 0, -1):
                #                 if row[personelUserHierarchyKey + str(index)].upper() == userEmail.upper():
                #                     user_level = index
                #                     break
                #             return user_level

                #         forecastDetailDataFrame["ul"] = forecastDetailDataFrame.apply(userLevel, axis=1)
                #         forecastDetailDataFrame = forecastDetailDataFrame[forecastDetailDataFrame.ul > 0]
                #         if ("forecastApprovals_" + forecast_type.upper() + "_" + snop_id) in cache:
                #             forecastApprovals = cache.get("forecastApprovals_" + forecast_type.upper() + "_" + snop_id)
                #             forecastApprovals = pd.DataFrame(forecastApprovals)
                #         else:
                #             forecastApprovals = ForecastCache.updateApprovals(forecast_type, configurations, snop_id, "")
                        
                #         if forecast_type.upper() != ForecastType(2).name:
                #             forecastApprovals = forecastApprovals[forecastApprovals[previousForecastType] == previousForecastUserHierarchyNoOfLevels]
                        
                #         forecastApprovals = forecastApprovals.astype({'forecast_header_id_fk_id':str})
                #         forecastApprovals.set_index(['forecast_header_id_fk_id'], inplace=True)
                #         forecastDetailDataFrame.set_index(['fhi'], inplace=True,drop=False)
                #         forecastDetailDataFrame = forecastDetailDataFrame.join(forecastApprovals, how='inner')
                        
                #         def st(row):
                #             status = np.where(
                #                 row['atl'] == forecastUserHierarchyNoOfLevels,
                #                 ForecastStatus(3).name,
                #                 np.where(
                #                     row['atl'] < row["ul"],
                #                     ForecastStatus(1).name,
                #                     ForecastStatus(2).name
                #                 )
                #             )
                #             return status

                #         forecastDetailDataFrame["st"] = st(forecastDetailDataFrame)
                #         dropColumns = ['forecast_approval_id']
                #         for index in range(1, int(configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(1).name][EntitiesConfigurationKeys(2).name])+1):
                #             dropColumns.append("ed"+str(index))
                #         forecastDetailDataFrame = forecastDetailDataFrame.drop(columns=dropColumns)
                #         ForecastCache.generateForecastCSV(forecastDetailDataFrame, forecast_type, configurations, snop_id, forecastUserHierarchyNoOfLevels, personelUserHierarchyKey, user_id)
                #         response = {'responseData': {'data': {"de": forecastDetailDataFrame.to_dict(orient='records')}, 'responseCode': ResponseCodes(12).name, 'responseMessage': 'Successfull'}}
                #         gzipResponse = json.dumps(response, cls=NumpyArrayEncoder, separators=(',', ':'))
                #         gzipResponse = gzip.compress(bytes(str(gzipResponse), 'utf-8'), 1, mtime=0)
                #         response = HttpResponse(gzipResponse, status)
                #         response['Content-Encoding'] = 'gzip'
                #         response['Content-Length'] = str(len(gzipResponse))
                #         cache.set(forecastDetailsUserKey, response)
            # cache.set(("forecastDetails_" + forecast_type.upper() + "_" + snop_id), snappy.compress(msgspec.json.encode(forecastDataFrame)))
            # cache.set(("forecastDetails_" + forecast_type.upper() + "_" + snop_id), json.dumps(forecastDataFrame, separators=(',', ':')))
        return forecastDataFrame

    # def updateApprovals(forecast_type, configurations, snop_id, tenant_id):
    #     if tenant_id != "":
    #         with tenant_context(get_tenant_model().objects.get(tenant_id=tenant_id)):
    #             return ForecastCache.updateForecastApprovals(forecast_type, configurations, snop_id)
    #     else:
    #         return ForecastCache.updateForecastApprovals(forecast_type, configurations, snop_id)

    # def updateForecastApprovals(forecast_type, configurations, snop_id):
    #     files = File.objects.filter(file_type__in=[FileType(3).name, FileType(5).name, FileType(6).name], snop_id_fk_id=snop_id, is_active=True).values('file_type', 'file_name')
    #     sales_history_forecast_mapping_file_path = ""
    #     if files:
    #         for fl in files:
    #             if fl["file_type"] == FileType(3).name:
    #                 sales_history_forecast_mapping_file_path = fl["file_name"]
    #                 break

    #     forecastDataFrame = []
    #     if sales_history_forecast_mapping_file_path:
    #         sales_history_forecast_mapping_result = get(sales_history_forecast_mapping_file_path).content
    #         # sales_history_forecast_mapping_result = gzip.decompress(sales_history_forecast_mapping_result)
    #         sales_history_forecast_mapping_result = ZstdReadWriter.decompressZSDContent(sales_history_forecast_mapping_result)
    #         sales_history_forecast_mapping_result = msgspec.json.decode(sales_history_forecast_mapping_result)
    #         forecastDataFrame = pd.DataFrame(sales_history_forecast_mapping_result)
    #         forecastHeaderIds = forecastDataFrame['fhi']

    #         previousForecastType = ForecastType(Util.castForecastType(forecast_type)-1).name
    #         forecastApprovals = ForecastApproval.objects.filter(forecast_header_id_fk_id__in = forecastHeaderIds, forecast_type__in=[forecast_type.upper(), previousForecastType]).values('forecast_type', 'approved_till_level', 'forecast_header_id_fk_id', 'forecast_approval_id')
    #         forecastApprovals = pd.DataFrame(forecastApprovals)
    #         forecastApprovalsPivot = forecastApprovals[['forecast_type', 'approved_till_level', 'forecast_header_id_fk_id', 'forecast_approval_id']]
    #         forecastApprovalsPivot = pd.pivot_table(forecastApprovalsPivot, columns='forecast_type', index=['forecast_header_id_fk_id'], values='approved_till_level').reset_index()
    #         forecastApprovalsForecastType = forecastApprovals[forecastApprovals.forecast_type == forecast_type.upper()][['forecast_header_id_fk_id', 'forecast_approval_id']]
    #         forecastApprovals = pd.merge(forecastApprovalsPivot, forecastApprovalsForecastType, how="inner", left_on=["forecast_header_id_fk_id"], right_on=["forecast_header_id_fk_id"])
    #         forecastApprovals = forecastApprovals.rename(columns={forecast_type.upper(): 'atl'})
    #         cache.set(("forecastApprovals_" + forecast_type.upper() + "_" + snop_id), forecastApprovals.to_dict(orient='records'))
    #         return forecastApprovals
    #     else:
    #         return []

    def update(forecast_type, configurations, snop_id, tenant_id):
        if tenant_id != "":
            with tenant_context(get_tenant_model().objects.get(tenant_id=tenant_id)):
                ForecastCache.updateForecast(forecast_type, configurations, snop_id)
        else:
            ForecastCache.updateForecast(forecast_type, configurations, snop_id)

    def updateForecast(forecast_type, configurations, snop_id):
        ForecastCache.updateForecastDetails(forecast_type, configurations, snop_id)
        # ForecastCache.updateForecastApprovals(forecast_type, configurations, snop_id)

    def generateForecastCSV(forecastDataFrame, forecast_type, configurations, snop_id, forecastUserHierarchyNoOfLevels, personelUserHierarchyKey, user_id):
        if user_id == "":
            planningFrequency = configurations[SnopConfigurationKeys(1).name][SnopConfigurationKeys(3).name]
            requiredColumns = ['fhi', 'si', 'ni', 'ci', 'sn', 'n', 'cn', 'sg', 'va', 'a', 's1', 'asvo', 'asv', 'lasvo', 'lasv', 'rc', 'rd', 'fd']
            renameColumns = {'rc': 'Remark Code', 'rd': 'Remark Description', 'sg': 'Segment', 'n': configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(12).name][EntitiesConfigurationKeys(18).name], 'sn': configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(9).name][EntitiesConfigurationKeys(18).name], 'cn': 'Channel Name', 'si': 'Sku Id', 'va': 'Variability', 'ci': 'Channel Id', 'ni': 'Node Id', 'lasv': 'Last ' + configurations[SnopConfigurationKeys(1).name][SnopConfigurationKeys(3).name] + ' Actual Sales Value', 'lasvo': 'Last ' + configurations[SnopConfigurationKeys(1).name][SnopConfigurationKeys(3).name] + ' Actual Sales Volume', 'asv': 'Average ' + configurations[SnopConfigurationKeys(1).name][SnopConfigurationKeys(3).name] + ' Sales Value', 'asvo': 'Average ' + configurations[SnopConfigurationKeys(1).name][SnopConfigurationKeys(3).name] + ' Sales Volume', 'a': 'Accuracy', 's1': 'Sales Manager'}
            if ForecastConfigurationKeys(16).name in configurations[ForecastConfigurationKeys(2).name]:
                extraProductFields = configurations[ForecastConfigurationKeys(2).name][ForecastConfigurationKeys(16).name][ForecastConfigurationKeys(18).name]
                extraLocationFields = configurations[ForecastConfigurationKeys(2).name][ForecastConfigurationKeys(16).name][ForecastConfigurationKeys(17).name]
                extraProductFieldsDefinition = configurations[ForecastConfigurationKeys(2).name][ForecastConfigurationKeys(16).name][ForecastConfigurationKeys(19).name][ForecastConfigurationKeys(18).name]
                extraLocationFieldsDefinition = configurations[ForecastConfigurationKeys(2).name][ForecastConfigurationKeys(16).name][ForecastConfigurationKeys(19).name][ForecastConfigurationKeys(17).name]
                extraProductCol, extraLocationCol = [], []
                if len(extraProductFields) > 0:
                    extraProductCol.extend(extraProductFieldsDefinition.values())
                    for extraProductFieldsDefinitionKey in extraProductFieldsDefinition.keys():
                        if EntitiesConfigurationKeys(11).name in extraProductFieldsDefinitionKey:
                            renameColumns = merge(renameColumns, {extraProductFieldsDefinition[extraProductFieldsDefinitionKey]: configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(9).name][extraProductFieldsDefinitionKey]})
                        elif "sku" in extraProductFieldsDefinitionKey:
                            renameColumns = merge(renameColumns, {extraProductFieldsDefinition[extraProductFieldsDefinitionKey]: extraProductFieldsDefinitionKey.replace("_", " ").replace("sku", configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(9).name][EntitiesConfigurationKeys(18).name]).title()})
                        else:
                            renameColumns = merge(renameColumns, {extraProductFieldsDefinition[extraProductFieldsDefinitionKey]: extraProductFieldsDefinitionKey.replace("_", " ").title()})
                if len(extraLocationFields) > 0:
                    extraLocationCol.extend(extraLocationFieldsDefinition.values())
                    for extraLocationFieldsDefinitionKey in extraLocationFieldsDefinition.keys():
                        if EntitiesConfigurationKeys(14).name in extraLocationFieldsDefinitionKey:
                            renameColumns = merge(renameColumns, {extraLocationFieldsDefinition[extraLocationFieldsDefinitionKey]: configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(12).name][extraLocationFieldsDefinitionKey]})
                        elif "node" in extraProductFieldsDefinitionKey:
                            renameColumns = merge(renameColumns, {extraProductFieldsDefinition[extraProductFieldsDefinitionKey]: extraProductFieldsDefinitionKey.replace("_", " ").replace("node", configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(12).name][EntitiesConfigurationKeys(18).name]).title()})
                        else:
                            renameColumns = merge(renameColumns, {extraProductFieldsDefinition[extraProductFieldsDefinitionKey]: extraProductFieldsDefinitionKey.replace("_", " ").title()})
            extraSalesmanHierarchyCol = [personelUserHierarchyKey + str(i) for i in range(forecastUserHierarchyNoOfLevels, 0, -1)]
            if extraProductCol:
                index_to_add = requiredColumns.index('sn') + 1
                requiredColumns = requiredColumns[:index_to_add] + extraProductCol + requiredColumns[index_to_add:]
            if extraLocationCol:
                index_to_add = requiredColumns.index('n') + 1
                requiredColumns = requiredColumns[:index_to_add] + extraLocationCol + requiredColumns[index_to_add:]
            if extraSalesmanHierarchyCol:
                index_to_add = requiredColumns.index('cn') + 1
                requiredColumns = requiredColumns[:index_to_add] + extraSalesmanHierarchyCol + requiredColumns[index_to_add:]

            forecastCSVDataFrame = forecastDataFrame[requiredColumns]
            forecastCSVDataFrame = forecastCSVDataFrame.reindex(columns=requiredColumns)

            """ format date as per planningFrequency"""
            def format_date(date, planningFrequency):
                if planningFrequency.upper() == PlanningFrequencies(1).name.upper():     #DAILY
                    date_obj = datetime.strptime(date, "%Y-%m-%d").strftime("%m/%d/%Y")
                elif planningFrequency.upper() == PlanningFrequencies(2).name.upper():   # FORTNIGHTLY
                    start_date = datetime.strptime(date, "%Y-%m-%d")
                    end_date = start_date + timedelta(days=14)
                    date_obj = f"{start_date.month}/{start_date.day}-{end_date.month}/{end_date.day}"
                elif planningFrequency.upper() == PlanningFrequencies(3).name.upper():   # WEEKLY
                    start_date = datetime.strptime(date, "%Y-%m-%d")
                    end_date = start_date + timedelta(days=6)
                    date_obj = f"{start_date.month}/{start_date.day}-{end_date.month}/{end_date.day}"
                elif planningFrequency.upper() == PlanningFrequencies(4).name.upper():   # MONTHLY
                    date_obj = datetime.strptime(date, "%Y-%m-%d").strftime("%b %y")
                elif planningFrequency.upper() == PlanningFrequencies(5).name.upper():   # QUATERLY
                    start_date = datetime.strptime(date, "%Y-%m-%d")
                    month_name = start_date.strftime("%b")
                    date_obj = start_date.strftime(f"%e {month_name.capitalize()} %Y")
                return date_obj

            for index, i in forecastCSVDataFrame['fd'].iteritems():
                i = str(i)
                for date, vol, val in zip(eval(i)['s']['p'], eval(i)['s']['vo'], eval(i)['s']['v']):
                    date_obj = format_date(date, planningFrequency)
                    forecastCSVDataFrame.at[index, "Statistical Volume (" + date_obj + ")"] = vol
                    forecastCSVDataFrame.at[index, "Statistical Value (" + date_obj + ")"] = val
                for date, vol, val in zip(eval(i)['o']['p'], eval(i)['o']['vo'], eval(i)['o']['v']):
                    date_obj = format_date(date, planningFrequency)
                    forecastCSVDataFrame.at[index, "Operational Volume (" + date_obj + ")"] = vol
                    forecastCSVDataFrame.at[index, "Operational Value (" + date_obj + ")"] = val
                if forecast_type.upper() == ForecastType(3).name:
                    for date, vol, val in zip(eval(i)['sa']['p'], eval(i)['sa']['vo'], eval(i)['sa']['v']):
                        date_obj = format_date(date, planningFrequency)
                        forecastCSVDataFrame.at[index, "Sales Volume (" + date_obj + ")"] = vol
                        forecastCSVDataFrame.at[index, "Sales Value (" + date_obj + ")"] = val
                if forecast_type.upper() == ForecastType(4).name:
                    for date, vol, val in zip(eval(i)['u']['p'], eval(i)['u']['vo'], eval(i)['u']['v']):
                        date_obj = format_date(date, planningFrequency)
                        forecastCSVDataFrame.at[index, "Unconstrained Volume (" + date_obj + ")"] = vol
                        forecastCSVDataFrame.at[index, "Unconstrained Value (" + date_obj + ")"] = val

            forecastCSVDataFrame = forecastCSVDataFrame.drop(columns=["fd"])
            col_to_move = ['rc', 'rd']
            for col in col_to_move:
                col_index = forecastCSVDataFrame.columns.get_loc(col)
                col_values = forecastCSVDataFrame.pop(col)
                forecastCSVDataFrame.insert(len(forecastCSVDataFrame.columns), col, col_values)

            # Rename the columns
            forecastCSVDataFrame = forecastCSVDataFrame.rename(columns=renameColumns)
            Util.createUploadCSV(f"Forecast_{forecast_type}_{snop_id}.csv", json.loads(forecastCSVDataFrame.to_json()), settings.BLOB_MASTER_FILES_CONTAINER)
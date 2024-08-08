import asyncio
from django.db.models import Q
from django.core.paginator import Paginator
from com_scai_dpai.utils import Util as base_util
from app.utils import Util
from com_scai_dpai.helper.configuration import Configuration
from app.enum import ForecastConfigurationKeys, EntitiesConfigurationKeys, ForecastType, ResponseCodes, NetworkAttributeNames, ProductAttributeNames, LocationAttributeNames, SegmentationType, ForecastStatus, PersonnelAttributeNames, EmailTemplate, AlertType, AlertConfigurationKeys, FileType
from app.helper.entities.personnel import Personnel
from app.helper.transactional.forecastSummary import ForecastSummary
from rest_framework import status
from app.model.forecast.forecast_detail import ForecastDetail
from app.serializers.forecast.forecast_detail import ForecastDetailSerializer
import logging
from app.model.forecast.forecast_header import ForecastHeader
from snop.models import Snop
from datetime import datetime
from com_scai_dpai.helper.login import Login
from django.conf import settings
import pandas as pd
import time
from datetime import datetime
import json
from com_scai_dpai.helper.email import Email
from app.model.forecast.alert import Alert
from app.serializers.forecast.alert import AlertSerializer
logger = logging.getLogger(__name__)
import threading
from django.core.cache import cache
from app.helper.cache.forecast import ForecastCache
import pyarrow as pa
import snappy
from app.model.common.file import File
import msgspec
from django.db import transaction
import zstandard as zstd

class ForecastReviewService():

    def approve(self, tenant_id, bu_id, snop_id, data, token, forecast_type):
        try:
            sysToken = Login.get_token(settings.SYS_ADMIN_EMAIL, settings.SYS_ADMIN_PASSWORD)
            logger.info(f"FORECAST-REVIEW START {snop_id}: {datetime.now()}")
            configurations = Configuration.get(tenant_id, bu_id, token)
            logger.info(f"FORECAST-REVIEW AFTER CONFIGURATION {snop_id}: {datetime.now()}")
            if ((forecast_type.upper() == ForecastType(2).name or forecast_type.upper() == ForecastType(4).name) and configurations[ForecastConfigurationKeys(2).name][ForecastConfigurationKeys(5).name]) or forecast_type.upper() == ForecastType(3).name:
                snop = Snop.objects.get(snop_id=snop_id, bu_id=bu_id, is_active=True)
                logger.info(f"FORECAST-REVIEW AFTER SNOP {snop_id}: {datetime.now()}")
                if data and snop: #and Util.isSnopActive(self, snop):
                    userEmail = base_util.getLoggedInUserEmailAddress(self, token)
                    previousForecastUserNoOfLevels = 0
                    if(forecast_type.upper() == ForecastType(2).name or forecast_type.upper() == ForecastType(4).name):
                        forecastUserHierarchyNoOfLevels = int(configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(1).name][EntitiesConfigurationKeys(2).name])
                        personelUserHierarchyKey = "ed"
                        personnelUserLevelKey = EntitiesConfigurationKeys(15).name
                        personnelUserNameKey = EntitiesConfigurationKeys(6).name
                        nextPersonnelUserLevelKey = EntitiesConfigurationKeys(16).name
                        nextPersonnelUserNameKey = EntitiesConfigurationKeys(5).name
                        if forecast_type.upper() == ForecastType(4).name:
                            previousForecastUserNoOfLevels = configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(7).name][EntitiesConfigurationKeys(8).name]
                    else:
                        forecastUserHierarchyNoOfLevels = int(configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(7).name][EntitiesConfigurationKeys(8).name])
                        personelUserHierarchyKey = "es"
                        personnelUserLevelKey = EntitiesConfigurationKeys(16).name
                        personnelUserNameKey = EntitiesConfigurationKeys(5).name
                        nextPersonnelUserLevelKey = EntitiesConfigurationKeys(15).name
                        nextPersonnelUserNameKey = EntitiesConfigurationKeys(6).name
                        previousForecastUserNoOfLevels = configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(1).name][EntitiesConfigurationKeys(2).name]
                    logger.info(f"FORECAST-REVIEW AFTER VARIABLES {snop_id}: {datetime.now()}")
                    if ("forecastDetails_" + forecast_type.upper() + "_" + snop_id) in cache:
                        dctx = zstd.ZstdDecompressor()
                        forecastDetailDataFrame = pa.deserialize_pandas(dctx.decompress(cache.get("forecastDetails_" + forecast_type.upper() + "_" + snop_id)))
                        forecastDetailDataFrame = forecastDetailDataFrame.astype({'fhi':str})
                        logger.info(f"FORECAST-REVIEW AFTER FORECAST DETAIL DATA FRAME {snop_id}: {datetime.now()}")
                        forecastDetailDataFrame = forecastDetailDataFrame[forecastDetailDataFrame.fhi.isin(list(set(data['forecastHeaders'])))]
                        logger.info(f"FORECAST-REVIEW AFTER FORECAST DETAIL DATA FRAME ISIN FILTER {snop_id}: {datetime.now()}")
                        logger.info(f"FORECAST-REVIEW AFTER GET APPROVALS {snop_id}: {datetime.now()}")
                        logger.info(f"FORECAST-REVIEW AFTER APPROVAL DATAFRAME {snop_id}: {datetime.now()}")
                        if not forecastDetailDataFrame.empty: # and not forecastApprovals.empty:
                            def userLevel(row):
                                user_level=0
                                for index in range(forecastUserHierarchyNoOfLevels, 0, -1):
                                    if row[personelUserHierarchyKey + str(index)].upper() == userEmail.upper():
                                        user_level = index
                                        break
                                return user_level

                            forecastDetailDataFrame["ul"] = forecastDetailDataFrame.apply(userLevel, axis=1)
                            logger.info(f"FORECAST-REVIEW AFTER UL {snop_id}: {datetime.now()}")
                            forecastDetailDataFrame = forecastDetailDataFrame[(forecastDetailDataFrame.ul > 0) & (forecastDetailDataFrame[forecast_type.lower() + '_approved_till_level'] < forecastDetailDataFrame.ul)]
                            logger.info(f"FORECAST-REVIEW AFTER JOIN {snop_id}: {datetime.now()}")
                            if not forecastDetailDataFrame.empty:
                                forecastDetailDataFrame = forecastDetailDataFrame[['fhi', 'ul', 'sn', 'cn', forecast_type.lower() + '_approved_till_level', 'n', 'si', 'ni', 'ci', 'fd']]
                                files = File.objects.filter(file_type__in=[FileType(1).name], snop_id_fk=snop, is_active=True).values('file_type', 'file_name')
                                logger.info(f"FORECAST-REVIEW AFTER FILES {snop_id}: {datetime.now()}")
                                personnel_file_path = ""
                                if files:
                                    for fl in files:
                                        if fl["file_type"] == FileType(1).name:
                                            personnel_file_path = fl["file_name"]
                                            break
                                if personnel_file_path:
                                    personnelDataDrame = pd.read_csv(personnel_file_path)
                                    forecastDetailDataFrame = pd.merge(forecastDetailDataFrame, personnelDataDrame, how="inner",left_on=["si","ni","ci"],right_on=["sku_id", "node_id", "channel_id"])
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
                                forecastDetailDataFrame = result
                                logger.info(f"FORECAST-REVIEW AFTER PERSONNEL DATAFRAME MERGE {snop_id}: {datetime.now()}")
                                if (AlertConfigurationKeys(1).name in configurations):
                                    isEmailAlertEnabled = configurations[AlertConfigurationKeys(1).name][AlertConfigurationKeys(2).name][AlertConfigurationKeys(3).name]
                                else:
                                    isEmailAlertEnabled = True
                                updatedForecastHeaders = []
                                updatedForecastHeadersUnconstrained = []
                                userId = base_util.getLoggedInUserId(token)
                                if forecast_type == ForecastType(2).name:
                                    abbreviated_forecast_type = "o"
                                elif forecast_type == ForecastType(3).name:
                                    abbreviated_forecast_type = "sa"
                                elif forecast_type == ForecastType(1).name:
                                    abbreviated_forecast_type = "s"
                                elif forecast_type == ForecastType(4).name:
                                    abbreviated_forecast_type = "u"
                                updatedForecastDetails = []
                                completedForecastHeaders = []
                                completedForecastHeaderIds = []
                                response = []
                                emailData = []
                                logger.info(f"FORECAST-REVIEW BEFORE LOOP START {snop_id}: {datetime.now()}")
                                for forecastDetail in forecastDetailDataFrame:
                                    updatedForecastHeaders.extend([{"updated_by": userId,
                                                                        forecast_type.lower() + '_approved_till_level': forecastDetail["ul"],
                                                                        "forecast_header_id": forecastDetail["fhi"]
                                                                        }])

                                    response.extend([{"atl": forecastDetail["ul"],
                                                    "fhi": forecastDetail["fhi"],
                                                    "st": ForecastStatus(3).name if forecastDetail["ul"] == forecastUserHierarchyNoOfLevels else ForecastStatus(2).name}])
                                    if isEmailAlertEnabled and not (forecastUserHierarchyNoOfLevels == forecastDetail["ul"] and forecast_type.upper() == ForecastType(4).name):
                                        emailData.extend(
                                            [{
                                                "product": forecastDetail["sn"],
                                                "location": forecastDetail["n"],
                                                "channel": forecastDetail["cn"],
                                                "to": forecastDetail[nextPersonnelUserLevelKey + "1"] if forecastUserHierarchyNoOfLevels == forecastDetail["ul"] else forecastDetail[personnelUserLevelKey + str(forecastDetail["ul"] + 1)],
                                                "receipientName": forecastDetail[nextPersonnelUserNameKey + "1"] if forecastUserHierarchyNoOfLevels == forecastDetail["ul"] else forecastDetail[personnelUserNameKey + str(forecastDetail["ul"] + 1)],
                                                "cc": forecastDetail[personnelUserLevelKey + str(forecastDetail["ul"])],
                                                "salesmanName": forecastDetail[personnelUserNameKey + str(forecastDetail["ul"])]
                                            }]
                                        )
                                    if forecastUserHierarchyNoOfLevels == forecastDetail["ul"] and forecast_type.upper() != ForecastType(4).name:
                                        detailLength = len(forecastDetail["fd"][abbreviated_forecast_type]["fdi"])
                                        for detailIndex in range(detailLength):
                                            updatedForecastDetails.extend([{
                                                "created_by": userId,
                                                "updated_by": userId,
                                                "volume": forecastDetail["fd"][abbreviated_forecast_type]["vo"][detailIndex], 
                                                "value": forecastDetail["fd"][abbreviated_forecast_type]["v"][detailIndex],
                                                "forecast_header_id_fk_id": forecastDetail["fhi"],
                                                "forecast_type": ForecastType(3).name if forecast_type.upper() == ForecastType(2).name else ForecastType(4).name,
                                                "period": forecastDetail["fd"][abbreviated_forecast_type]["p"][detailIndex]}])
                                            if forecast_type.upper() == ForecastType(3).name and not configurations[ForecastConfigurationKeys(2).name][ForecastConfigurationKeys(5).name]:
                                                updatedForecastHeadersUnconstrained.extend([{"updated_by": userId,
                                                    ForecastType(4).name.lower() + '_approved_till_level': int(previousForecastUserNoOfLevels),
                                                    "forecast_header_id": forecastDetail["fhi"]}])
                                    elif forecastUserHierarchyNoOfLevels == forecastDetail["ul"]:
                                        completedForecastHeaders.extend([forecastDetail])
                                        completedForecastHeaderIds.extend([
                                            forecastDetail["fhi"]])
                                logger.info(f"FORECAST-REVIEW AFTER LOOP {snop_id}: {datetime.now()}")
                                with transaction.atomic(using='default'):
                                    batch_size = 100
                                    for i in range(0, len(updatedForecastHeaders), batch_size):
                                        batch = updatedForecastHeaders[i:i + batch_size]
                                        objects_list = [ForecastHeader(**batch[data]) for data in range(0, len(batch))]
                                        ForecastHeader.objects.bulk_update(objects_list, fields = [forecast_type.lower() + '_approved_till_level', 'updated_by'])
                                    
                                    batch_size = 100
                                    for i in range(0, len(updatedForecastHeadersUnconstrained), batch_size):
                                        batch = updatedForecastHeadersUnconstrained[i:i + batch_size]
                                        objects_list = [ForecastHeader(**batch[data]) for data in range(0, len(batch))]
                                        ForecastHeader.objects.bulk_update(objects_list, fields = [ForecastType(4).name.lower() + '_approved_till_level', 'updated_by'])

                                    logger.info(f"FORECAST-REVIEW AFTER APPROVAL BULK UPDATE {snop_id}: {datetime.now()}")
                                    if updatedForecastDetails:
                                        batch_size = 30000
                                        for i in range(0, len(updatedForecastDetails), batch_size):
                                            batch = updatedForecastDetails[i:i + batch_size]
                                            objects_list = [ForecastDetail(**batch[data]) for data in range(0, len(batch))]
                                            ForecastDetail.objects.bulk_create(objects_list, ignore_conflicts=True)
                                        logger.info(f"FORECAST-REVIEW AFTER FORECAST DETAIL BULK CREATE {snop_id}: {datetime.now()}")
                                    else:
                                        if completedForecastHeaders and completedForecastHeaderIds:
                                            forecastSummary = ForecastReviewService.createForecastSummary(completedForecastHeaders,completedForecastHeaderIds)
                                            logger.info(f"FORECAST-REVIEW AFTER CREATE FORECAST SUMMARY {snop_id}: {datetime.now()}")
                                            ForecastSummary.post(tenant_id, bu_id, snop.snop_id, forecastSummary,sysToken)
                                    if isEmailAlertEnabled:
                                        ForecastReviewService.triggerEmail(self, tenant_id, sysToken, emailData,EmailTemplate(1).name,settings.PLATFORM_URL, configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(9).name]["base"], configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(12).name]["base"])
                                        logger.info(f"FORECAST-REVIEW AFTER TRIGGER EMAIL {snop_id}: {datetime.now()}")
                                if updatedForecastDetails:
                                    threading.Thread(target=ForecastCache.update, args=(ForecastType(3).name if forecast_type.upper() == ForecastType(2).name else ForecastType(4).name, configurations, snop_id, tenant_id)).start()
                                threading.Thread(target=ForecastCache.update, args=(forecast_type, configurations, snop_id, tenant_id)).start()
                                return {"responseCode": ResponseCodes(13).name,"responseMessage": "Forecast Approved Successfully","status": status.HTTP_200_OK, "data": response}
                            else:
                                logger.error(
                                    f"DPAI Service: {forecast_type}ForecastReviewService-APPROVE: Data Validation Failed: tenant_id: {tenant_id} | bu_id: {bu_id} | snop_id: {snop_id} | request body: {data}")
                                return {"responseCode": ResponseCodes(5).name, "responseMessage": "Invalid Request Body", "status": status.HTTP_400_BAD_REQUEST}
                        else:
                            logger.error(
                                f"DPAI Service: {forecast_type}ForecastReviewService-APPROVE: Data Validation Failed: tenant_id: {tenant_id} | bu_id: {bu_id} | snop_id: {snop_id} | request body: {data}")
                            return {"responseCode": ResponseCodes(5).name, "responseMessage": "Invalid Request Body", "status": status.HTTP_400_BAD_REQUEST}
                    else:
                        logger.error(
                            f"DPAI Service: {forecast_type}ForecastReviewService-APPROVE: Data Validation Failed: tenant_id: {tenant_id} | bu_id: {bu_id} | snop_id: {snop_id} | request body: {data}")
                        return {"responseCode": ResponseCodes(5).name, "responseMessage": "Invalid Request Body", "status": status.HTTP_400_BAD_REQUEST}
                else:
                    logger.error(
                        f"DPAI Service: {forecast_type}ForecastReviewService-APPROVE: Invalid Request Body: tenant_id: {tenant_id} | bu_id: {bu_id} | snop_id: {snop_id}")
                    return {"responseCode": ResponseCodes(6).name, "responseMessage": "Invalid Request Body","status": status.HTTP_400_BAD_REQUEST}
            else:
                logger.error(
                    f"DPAI Service: {forecast_type}ForecastReviewService-APPROVE: Forecast is not enabled: tenant_id: {tenant_id} | bu_id: {bu_id} | snop_id: {snop_id}")
                return {"responseCode": ResponseCodes(3).name, "responseMessage": "Forecasting is not enabled","status": status.HTTP_400_BAD_REQUEST}
        except Exception as e:
            logger.error(f"DPAI Service: ForecastReviewService-APPROVE: Forecast Data approved with error: {e}")
            return {}
                
    def reject(self, tenant_id, bu_id, snop_id, data, alertCode, alertDescription, token, forecast_type):
        try:
            sysToken = Login.get_token(settings.SYS_ADMIN_EMAIL, settings.SYS_ADMIN_PASSWORD)
            configurations = Configuration.get(tenant_id, bu_id, sysToken)
            snop = {}
            if ((forecast_type.upper() == ForecastType(2).name or forecast_type.upper() == ForecastType(4).name) and configurations[ForecastConfigurationKeys(2).name][ForecastConfigurationKeys(5).name]) or forecast_type.upper() == ForecastType(3).name:
                snop = Snop.objects.get(snop_id=snop_id, bu_id=bu_id, is_active=True)
                if data and snop: #and Util.isSnopActive(self, snop):
                    userEmail = base_util.getLoggedInUserEmailAddress(self, token)
                    previousForecastUserNoOfLevels = 0
                    if(forecast_type.upper() == ForecastType(2).name or forecast_type.upper() == ForecastType(4).name):
                        forecastUserHierarchyNoOfLevels = int(configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(1).name][EntitiesConfigurationKeys(2).name])
                        personelUserHierarchyKey = "ed"
                        personnelUserLevelKey = EntitiesConfigurationKeys(15).name
                        personnelUserNameKey = EntitiesConfigurationKeys(6).name
                        nextPersonnelUserLevelKey = EntitiesConfigurationKeys(16).name
                        nextPersonnelUserNameKey = EntitiesConfigurationKeys(5).name
                        if forecast_type.upper() == ForecastType(4).name:
                            previousForecastUserNoOfLevels = configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(7).name][EntitiesConfigurationKeys(8).name]
                    else:
                        forecastUserHierarchyNoOfLevels = int(configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(7).name][EntitiesConfigurationKeys(8).name])
                        personelUserHierarchyKey = "es"
                        personnelUserLevelKey = EntitiesConfigurationKeys(16).name
                        personnelUserNameKey = EntitiesConfigurationKeys(5).name
                        nextPersonnelUserLevelKey = EntitiesConfigurationKeys(15).name
                        nextPersonnelUserNameKey = EntitiesConfigurationKeys(6).name
                        previousForecastUserNoOfLevels = configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(1).name][EntitiesConfigurationKeys(2).name]

                    if ("forecastDetails_" + forecast_type.upper() + "_" + snop_id) in cache:
                        dctx = zstd.ZstdDecompressor()
                        forecastDetailDataFrame = pa.deserialize_pandas(dctx.decompress(cache.get("forecastDetails_" + forecast_type.upper() + "_" + snop_id)))
                        # forecastDetailDataFrame = pa.deserialize_pandas(snappy.decompress(cache.get("forecastDetails_" + forecast_type.upper() + "_" + snop_id)))
                        forecastDetailDataFrame = forecastDetailDataFrame.astype({'fhi':str})
                        forecastDetailDataFrame = forecastDetailDataFrame[forecastDetailDataFrame.fhi.isin(list(set(data)))]
                        
                        if not forecastDetailDataFrame.empty:
                            def userLevel(row):
                                user_level=0
                                for index in range(forecastUserHierarchyNoOfLevels, 0, -1):
                                    if row[personelUserHierarchyKey + str(index)].upper() == userEmail.upper():
                                        user_level = index
                                        break
                                return user_level

                            forecastDetailDataFrame["ul"] = forecastDetailDataFrame.apply(userLevel, axis=1)
                            forecastDetailDataFrame = forecastDetailDataFrame[(forecastDetailDataFrame.ul > 0) & (forecastDetailDataFrame[forecast_type.lower() + '_approved_till_level'] < forecastDetailDataFrame.ul)]
                            # forecastApprovals = ForecastApproval.objects.filter(forecast_header_id_fk_id__in = forecastDetailDataFrame['fhi'], forecast_type = forecast_type.upper()).values('forecast_header_id_fk_id','approved_till_level', 'forecast_approval_id')
                            # forecastApprovals = pd.DataFrame(forecastApprovals)
                            # if not forecastApprovals.empty:
                            #     forecastApprovals.set_axis(['forecast_header_id_fk_id', 'atl', 'forecast_approval_id'], axis='columns', inplace=True)
                            #     forecastApprovals = forecastApprovals.astype({'forecast_header_id_fk_id':str})
                            #     forecastApprovals.set_index(['forecast_header_id_fk_id'], inplace=True)
                            #     forecastDetailDataFrame.set_index(['fhi'], inplace=True,drop=False)
                            #     forecastDetailDataFrame = forecastDetailDataFrame.join(forecastApprovals, how='inner')
                            #     forecastDetailDataFrame = forecastDetailDataFrame[forecastDetailDataFrame.atl < forecastDetailDataFrame.ul]
                            if not forecastDetailDataFrame.empty:
                                forecastDetailDataFrame = forecastDetailDataFrame[['fhi', 'ul', 'sn', 'cn', forecast_type.lower() + '_approved_till_level', 'n', 'si', 'ni', 'ci', 'fd']]
                                files = File.objects.filter(file_type__in=[FileType(1).name], snop_id_fk=snop, is_active=True).values('file_type', 'file_name')
                                personnel_file_path = ""
                                if files:
                                    for fl in files:
                                        if fl["file_type"] == FileType(1).name:
                                            personnel_file_path = fl["file_name"]
                                            break
                                if personnel_file_path:
                                    personnelDataDrame = pd.read_csv(personnel_file_path)
                                    forecastDetailDataFrame = pd.merge(forecastDetailDataFrame, personnelDataDrame, how="inner",left_on=["si","ni","ci"],right_on=["sku_id", "node_id", "channel_id"])
                                forecastDetailDataFrame = msgspec.json.decode(forecastDetailDataFrame.to_json(orient='records',default_handler=str,date_format='iso'))
                                if(AlertConfigurationKeys(1).name in configurations): 
                                    isEmailAlertEnabled = configurations[AlertConfigurationKeys(1).name][AlertConfigurationKeys(2).name][AlertConfigurationKeys(3).name] 
                                else:
                                    isEmailAlertEnabled = True
                                updatedForecastHeaders = []
                                userId = base_util.getLoggedInUserId(token)
                                response = []
                                emailData = []
                                alerts = []
                                for forecastDetail in forecastDetailDataFrame:
                                    if forecastDetail[forecast_type.lower() + '_approved_till_level'] not in [forecastUserHierarchyNoOfLevels, 0]:
                                        updatedForecastHeaders.extend([{"updated_by": userId, forecast_type.lower() + "_approved_till_level": forecastDetail[forecast_type.lower() + '_approved_till_level'] - 1, "forecast_heeader_id": forecastDetail["fhi"]}])
                                        response.append({"atl": forecastDetail[forecast_type.lower() + '_approved_till_level'] - 1,
                                                "fhi": forecastDetail["fhi"]})
                                        alerts.append(Alert(forecast_header_id_fk_id= forecastDetail["fhi"], alert_code= alertCode, forecast_type=forecast_type.upper(), alert_type= AlertType(1).name, user_level= forecastDetail["ul"], alert_description= alertDescription, created_at= datetime.now(), updated_at= datetime.now(), created_by= userId, updated_by= userId))
                                        if isEmailAlertEnabled:
                                            emailData.append(
                                                {
                                                    "product": forecastDetail["sn"],
                                                    "location": forecastDetail["n"],
                                                    "channel": forecastDetail["cn"],
                                                    "to": forecastDetail[personnelUserLevelKey + str(forecastDetail["atl"])],
                                                    "receipientName": forecastDetail[personnelUserNameKey + str(forecastDetail["atl"])],
                                                    "cc": forecastDetail[personnelUserLevelKey + str(forecastDetail["ul"])],
                                                    "salesmanName": forecastDetail[personnelUserNameKey + str(forecastDetail["ul"])]
                                                },
                                            )
                                    else:
                                        logger.error(f"DPAI Service: {forecast_type}ForecastReviewService-REJECT: Data Validation Failed: tenant_id: {tenant_id} | bu_id: {bu_id} | snop_id: {snop_id} | request body: {data}")
                                        return {"responseCode": ResponseCodes(2).name, "responseMessage": "Unauthorized!", "status": status.HTTP_401_UNAUTHORIZED}
                                with transaction.atomic(using='default'):
                                    batch_size = 100
                                    for i in range(0, len(updatedForecastHeaders), batch_size):
                                        batch = updatedForecastHeaders[i:i + batch_size]
                                        objects_list = [ForecastHeader(**batch[data]) for data in range(0, len(batch))]
                                        ForecastHeader.objects.bulk_update(objects_list, batch_size=batch_size, fields = [forecast_type.lower() + '_approved_till_level', 'updated_by'])

                                    if alerts:
                                        Alert.objects.bulk_create(alerts, 1000, ignore_conflicts=True)
                                    if isEmailAlertEnabled:
                                        ForecastReviewService.triggerEmail(self,tenant_id, sysToken, emailData, EmailTemplate(2).name, settings.PLATFORM_URL, configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(9).name]["base"], configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(12).name]["base"])
                                threading.Thread(target=ForecastCache.update, args=(forecast_type, configurations, snop_id, tenant_id)).start()
                                return {"responseCode": ResponseCodes(15).name, "responseMessage": "Forecast Rejection completed Successful", "status": status.HTTP_200_OK, "data": response}
                            else:
                                logger.error(f"DPAI Service: {forecast_type}ForecastReviewService-REJECT: Data Validation Failed: tenant_id: {tenant_id} | bu_id: {bu_id} | snop_id: {snop_id} | request body: {data}")
                                return {"responseCode": ResponseCodes(5).name, "responseMessage": "Invalid Request Body", "status": status.HTTP_400_BAD_REQUEST}
                        else:
                            logger.error(f"DPAI Service: {forecast_type}ForecastReviewService-REJECT: Data Validation Failed: tenant_id: {tenant_id} | bu_id: {bu_id} | snop_id: {snop_id} | request body: {data}")
                            return {"responseCode": ResponseCodes(5).name, "responseMessage": "Invalid Request Body", "status": status.HTTP_400_BAD_REQUEST}
                    else:
                        logger.error(f"DPAI Service: {forecast_type}ForecastReviewService-REJECT: Data Validation Failed: tenant_id: {tenant_id} | bu_id: {bu_id} | snop_id: {snop_id} | request body: {data}")
                        return {"responseCode": ResponseCodes(5).name, "responseMessage": "Invalid Request Body", "status": status.HTTP_400_BAD_REQUEST}
                else:
                    logger.error(
                        f"DPAI Service: {forecast_type}ForecastReviewService-REJECT: Invalid Request Body: tenant_id: {tenant_id} | bu_id: {bu_id} | snop_id: {snop_id}")
                    return {"responseCode": ResponseCodes(6).name, "responseMessage": "Invalid Request Body",
                            "status": status.HTTP_400_BAD_REQUEST}
            else:
                logger.error(
                    f"DPAI Service: {forecast_type}ForecastReviewService-REJECT: Forecast is not enabled: tenant_id: {tenant_id} | bu_id: {bu_id} | snop_id: {snop_id}")
                return {"responseCode": ResponseCodes(3).name, "responseMessage": "Forecasting is not enabled",
                        "status": status.HTTP_400_BAD_REQUEST}
        except Exception as e:
            logger.error(f"DPAI Service: ForecastReviewService-REJECT: Forecast Data rejected with error: {e}")
            return {}

    def createForecastSummary(completedForecastHeaders, completedForecastHeaderIds):
        try:
            forecastDetails = ForecastDetail.objects.filter(
                forecast_header_id_fk_id__in=completedForecastHeaderIds).order_by('period').values('forecast_header_id_fk', 'period',
                                                                                    'forecast_type', 'volume', 'value')
            response = []
            for forecast in completedForecastHeaders:
                filteredForecastDetails = forecastDetails.filter(forecast_header_id_fk = forecast['fhi'])
                periods = list(filteredForecastDetails.values_list('period', flat=True).distinct('period'))
                for period in periods:
                    summary = {}
                    summary['date'] = period
                    summary["sku_id"] = forecast['si']
                    summary["node_id"] = forecast['ni']
                    summary["channel_id"] = forecast['ci']
                    for detail in filteredForecastDetails:
                        if period == detail['period']:
                            summary[detail['forecast_type'].lower() + '_forecast_volume'] = detail['volume']
                            summary[detail['forecast_type'].lower() + '_forecast_value'] = detail['value']
                    response.append(summary)
            return json.dumps(response, default=str)
        except Exception as e:
            logger.error(f"DPAI Service: createForecastSummary: createForecastSummary with error: {e}")
            return []

    def triggerEmail(self, tenant_id, sysToken, emailData, templateType, url, product_attribute_name, location_attribute_name):
        try:
            request_body = []
            email_dict = {}
            for data in emailData:
                key = (data["to"], data["cc"])
                if key not in email_dict:
                    email_dict[key] = {
                        "to": data["to"],
                        "cc": data["cc"],
                        "templateType": templateType,
                        "data": {
                            "receipientName": data["receipientName"],
                            "product_attribute_name": product_attribute_name,
                            "location_attribute_name": location_attribute_name,
                            "url": url,
                            "salesmanName": data["salesmanName"],
                            "data": []
                        }
                    }
                    request_body.append(email_dict[key])
                email_dict[key]["data"]["data"].append({
                    "product": data["product"],
                    "location": data["location"],
                    "channel": data["channel"]
                    })
            Email.post(tenant_id, request_body, sysToken)
            logger.info(f"TRIGGER EMAIL TASKS CREATED: {datetime.now()}")
        except Exception as e:
            logger.error(f"DPAI Service: triggerEmail: triggerEmail with error: {e}")
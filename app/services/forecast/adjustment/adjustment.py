from app.helper.utils.ZstdReadWriter import ZstdReadWriter
from com_scai_dpai.utils import Util as base_util
from app.utils import Util
from com_scai_dpai.helper.configuration import Configuration
from app.enum import ForecastConfigurationKeys, EntitiesConfigurationKeys, ForecastType, ResponseCodes, NetworkAttributeNames, ProductAttributeNames, LocationAttributeNames, SegmentationType, ForecastStatus, PersonnelAttributeNames, FileType
from app.helper.entities.personnel import Personnel
from rest_framework import status
from app.model.forecast.forecast_detail import ForecastDetail
from app.serializers.forecast.forecast_detail import ForecastDetailSerializer
import logging
from app.model.forecast.forecast_header import ForecastHeader
from snop.models import Snop
from datetime import datetime
from com_scai_dpai.helper.login import Login
from django.conf import settings
from app.helper.entities.product import Product
from app.helper.entities.location import Location
import pandas as pd
from app.model.forecast.adjustmentLog import AdjustmentLog
import json
from django.utils import timezone
from app.serializers.forecast.adjustmentLog import AdjustmentLogSerializer
logger = logging.getLogger(__name__)
from app.helper.cache.forecast import ForecastCache
from threading import Thread
import threading
import pyarrow as pa
import snappy
import msgspec
from django.core.cache import cache
from django.db import transaction
from app.model.common.file import File
import gzip
from requests import get
import zstandard as zstd

class ForecastAdjustmentService():

    def adjust(self, tenant_id, bu_id, snop_id, data, token, forecast_type):
        try:
            configurations = Configuration.get(tenant_id, bu_id, token)
            if ((forecast_type.upper() == ForecastType(2).name or forecast_type.upper() == ForecastType(4).name) and configurations[ForecastConfigurationKeys(2).name][ForecastConfigurationKeys(5).name]) or forecast_type.upper() == ForecastType(3).name:
                snop = Snop.objects.get(snop_id=snop_id, bu_id=bu_id, is_active=True)
                if data and snop: #and Util.isSnopActive(self, snop):
                    userEmail = base_util.getLoggedInUserEmailAddress(self, token)
                    if(forecast_type.upper() == ForecastType(2).name or forecast_type.upper() == ForecastType(4).name):
                        forecastUserHierarchyNoOfLevels = int(configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(1).name][EntitiesConfigurationKeys(2).name])
                        personelUserHierarchyKey = "ed"
                    else:
                        forecastUserHierarchyNoOfLevels = int(configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(7).name][EntitiesConfigurationKeys(8).name])
                        personelUserHierarchyKey = "es"

                    if ("forecastDetails_" + forecast_type.upper() + "_" + snop_id) in cache: # and ("forecastApprovals_" + forecast_type.upper() + "_" + snop_id) in cache:
                        dctx = zstd.ZstdDecompressor()
                        forecastDetailDataFrame = pa.deserialize_pandas(dctx.decompress(cache.get("forecastDetails_" + forecast_type.upper() + "_" + snop_id)))
                        # forecastDetailDataFrame = pa.deserialize_pandas(snappy.decompress(cache.get("forecastDetails_" + forecast_type.upper() + "_" + snop_id)))
                        # forecastApprovals = pd.DataFrame(cache.get("forecastApprovals_" + forecast_type.upper() + "_" + snop_id))
                        if not forecastDetailDataFrame.empty: # and not forecastApprovals.empty:
                            def userLevel(row):
                                user_level=0
                                for index in range(forecastUserHierarchyNoOfLevels, 0, -1):
                                    if row[personelUserHierarchyKey + str(index)].upper() == userEmail.upper():
                                        user_level = index
                                        break
                                return user_level

                            forecastDetailDataFrame["ul"] = forecastDetailDataFrame.apply(userLevel, axis=1)
                            # levels_columns = [personelUserHierarchyKey + str(i) for i in range(forecastUserHierarchyNoOfLevels, 0, -1)]
                            # levels = forecastDetailDataFrame[levels_columns].apply(lambda x: x.str.upper())

                            # forecastDetailDataFrame["ul"] = (levels == userEmail.upper()).idxmax(axis=1).str.replace(
                            #     personelUserHierarchyKey, "").astype(int)
                            forecastDetailDataFrame = forecastDetailDataFrame[(forecastDetailDataFrame.ul > 0) & (forecastDetailDataFrame[forecast_type.lower() + '_approved_till_level'] < forecastDetailDataFrame.ul)]
                            # forecastApprovals = forecastApprovals.astype({'forecast_header_id_fk_id':str})
                            # forecastApprovals.set_index(['forecast_header_id_fk_id'], inplace=True)
                            # forecastDetailDataFrame.set_index(['fhi'], inplace=True,drop=False)
                            # forecastDetailDataFrame = forecastDetailDataFrame.join(forecastApprovals, how='inner')
                            # forecastDetailDataFrame = forecastDetailDataFrame[forecastDetailDataFrame.atl < forecastDetailDataFrame.ul]
                            forecastDetailDataFrame = forecastDetailDataFrame[['fhi', 'up', 'ul', 'sn', 'cn', 'fd']]
                            forecastDetailDataFrame = forecastDetailDataFrame.astype({'fhi':str})
                            forecastDetailDataFrame = forecastDetailDataFrame[forecastDetailDataFrame.fhi.isin(list(set(data['forecastIds'])))]
                            forecastDetailDataFrame = msgspec.json.decode(forecastDetailDataFrame.to_json(orient='records',default_handler=str,date_format='iso'))
                            period = list(set(data['period']))
                            userId = base_util.getLoggedInUserId(token)
                            adjustmentLogsForecastHeaderIds = []
                            adjustmentLogIncreaseorDecrease = "+" if "INC" in data['operation'] else "-"
                            adjustmentLogAbsoluteOrPercent = "%" if "PERCENT" in data['operation'] else ""
                            updatedForecasts = []
                            adjustmentLogs = []
                            response = []
                            remarks = []
                            remarksForecastHeaderIds = []
                            remark_in_active = []
                            if forecast_type == ForecastType(2).name:
                                abbreviated_forecast_type = "o"
                            elif forecast_type == ForecastType(3).name:
                                abbreviated_forecast_type = "sa"
                            elif forecast_type == ForecastType(1).name:
                                abbreviated_forecast_type = "s"
                            elif forecast_type == ForecastType(4).name:
                                abbreviated_forecast_type = "u"
                            
                            current_datetime = datetime.now()
                            adjustmentLogsForecastHeaderIds = set()
                            remarksForecastHeaderIds = set()

                            for forecastDetail in forecastDetailDataFrame:
                                detailLength = len(forecastDetail["fd"][abbreviated_forecast_type]["fdi"])
                                for detailIndex in range(detailLength):
                                    if forecastDetail["fd"][abbreviated_forecast_type]["p"][detailIndex] in period:
                                        updatedVolume = Util.getVolume(self, forecastDetail["fd"][abbreviated_forecast_type]["vo"][detailIndex], data['operation'], data['adjustedVolume'])
                                        updatedValue = round(forecastDetail["up"] * int(updatedVolume), 2)
                                        updatedForecasts.append(
                                            ForecastDetail(
                                                updated_at=current_datetime,
                                                updated_by=userId,
                                                volume=updatedVolume,
                                                value=updatedValue,
                                                forecast_detail_id=forecastDetail["fd"][abbreviated_forecast_type]["fdi"][detailIndex]
                                            )
                                        )
                                        response.append({
                                            "volume": updatedVolume,
                                            "value": updatedValue,
                                            "forecast_detail_id": forecastDetail["fd"][abbreviated_forecast_type]["fdi"][detailIndex],
                                            "forecast_header_id_fk": forecastDetail["fhi"],
                                            "forecast_type": forecast_type.upper(),
                                            "period": forecastDetail["fd"][abbreviated_forecast_type]["p"][detailIndex]
                                        })

                                if forecastDetail["fhi"] not in adjustmentLogsForecastHeaderIds:
                                    adjustmentLogsForecastHeaderIds.add(forecastDetail["fhi"])
                                    adjustmentLogs.append(
                                        AdjustmentLog(
                                            forecast_header_id_fk_id=forecastDetail["fhi"],
                                            forecast_type=forecast_type.upper(),
                                            user_level=forecastDetail["ul"],
                                            adjustment_log_description=f"{userEmail} has adjusted {forecast_type.lower()} forecast for {forecastDetail['sn']}/{forecastDetail['cn']} by {adjustmentLogIncreaseorDecrease}{data['adjustedVolume']}{adjustmentLogAbsoluteOrPercent}",
                                            created_at=current_datetime,
                                            updated_at=current_datetime,
                                            created_by=userId,
                                            updated_by=userId
                                        )
                                    )

                                if forecastDetail["fhi"] not in remarksForecastHeaderIds:
                                    remarksForecastHeaderIds.add(forecastDetail["fhi"])
                                    remarks.append(
                                        ForecastHeader(
                                            forecast_header_id=forecastDetail["fhi"],
                                            remark_description=data["remarkDescription"],
                                            remark_code=data["remark"],
                                            updated_by=userId
                                        )
                                    )
                                    remark_in_active.append(forecastDetail["fhi"])
                            if updatedForecasts:
                                with transaction.atomic(using='default'):
                                    batch_size = 500
                                    for i in range(0, len(updatedForecasts), batch_size):
                                        batch = updatedForecasts[i:i+batch_size]
                                        ForecastDetail.objects.bulk_update(batch, fields=['updated_by', 'volume', 'value'], batch_size=batch_size)
                                    AdjustmentLog.objects.bulk_create(adjustmentLogs, ignore_conflicts=True,batch_size=500)
                                    
                                    if remarks:
                                        # Remark.objects.filter(forecast_header_id_fk_id__in=remark_in_active).update(is_active=False)
                                        ForecastHeader.objects.bulk_update(remarks, batch_size=500, fields=['remark_description', 'updated_by', 'remark_code'])
                                threading.Thread(target=ForecastCache.updateDetails, args=(forecast_type, configurations, snop_id, tenant_id)).start()
                                return {"responseCode":ResponseCodes(14).name,"responseMessage": "Forecast Adjusted Successfully", "status": status.HTTP_200_OK, "data": {"data": response, "remark": data["remark"] , "remarkDescription": data["remarkDescription"]}}
                            else:
                                logger.error(f"DPAI Service: {forecast_type}ForecastAdjustmentService-ADJUST: Data Validation Failed: tenant_id: {tenant_id} | bu_id: {bu_id} | snop_id: {snop_id} | request body: {data}")
                                return {"responseCode":ResponseCodes(5).name,"responseMessage": "Invalid Request Body", "status": status.HTTP_400_BAD_REQUEST}
                        else:
                            logger.error(f"DPAI Service: {forecast_type}ForecastAdjustmentService-ADJUST: Data Validation Failed: tenant_id: {tenant_id} | bu_id: {bu_id} | snop_id: {snop_id} | request body: {data}")
                            return {"responseCode":ResponseCodes(5).name,"responseMessage": "Invalid Request Body", "status": status.HTTP_400_BAD_REQUEST}
                    else:
                        logger.error(f"DPAI Service: {forecast_type}ForecastAdjustmentService-ADJUST: Invalid Request Body: tenant_id: {tenant_id} | bu_id: {bu_id} | snop_id: {snop_id}")
                        return {"responseCode":ResponseCodes(6).name,"responseMessage": "Invalid Request Body", "status": status.HTTP_400_BAD_REQUEST}
                else:
                    logger.error(f"DPAI Service: {forecast_type}ForecastAdjustmentService-ADJUST: Invalid Request Body: tenant_id: {tenant_id} | bu_id: {bu_id} | snop_id: {snop_id}")
                    return {"responseCode":ResponseCodes(6).name,"responseMessage": "Invalid Request Body", "status": status.HTTP_400_BAD_REQUEST}
            else:
                logger.error(f"DPAI Service: {forecast_type}ForecastAdjustmentService-ADJUST: Forecast is not enabled: tenant_id: {tenant_id} | bu_id: {bu_id} | snop_id: {snop_id}")
                return {"responseCode":ResponseCodes(3).name,"responseMessage": "Forecasting is not enabled", "status": status.HTTP_400_BAD_REQUEST}
        except Exception as e:
            logger.error(f"DPAI Service: ForecastAdjutsmentService Adjust: Forecast Data adjusted with error: {e}")
            return {}

    def get(self, tenant_id, bu_id, snop_id, token, forecast_type):
        try:
            configurations = Configuration.get(tenant_id, bu_id, token)
            if ((forecast_type.upper() == ForecastType(2).name or forecast_type.upper() == ForecastType(4).name) and configurations[ForecastConfigurationKeys(2).name][ForecastConfigurationKeys(5).name]) or forecast_type.upper() == ForecastType(3).name:
                snop = Snop.objects.get(snop_id=snop_id, bu_id=bu_id, is_active=True)
                if snop:
                    forecastDataFrame = []
                    if ("forecastDetails_" + forecast_type.upper() + "_" + snop_id) in cache:
                        dctx = zstd.ZstdDecompressor()
                        forecastDataFrame = pa.deserialize_pandas(dctx.decompress(cache.get("forecastDetails_" + forecast_type.upper() + "_" + snop_id)))
                        # forecastDataFrame = pa.deserialize_pandas(snappy.decompress(cache.get("forecastDetails_" + forecast_type.upper() + "_" + snop_id)))
                    else:
                        files = File.objects.filter(file_type__in=[FileType(3).name], snop_id_fk_id=snop_id, is_active=True).values('file_type', 'file_name')
                        sales_history_forecast_mapping_file_path = ""
                        if files:
                            for fl in files:
                                if fl["file_type"] == FileType(3).name:
                                    sales_history_forecast_mapping_file_path = fl["file_name"]
                                    break
                        
                        if sales_history_forecast_mapping_file_path:
                            sales_history_forecast_mapping_result = get(sales_history_forecast_mapping_file_path).content
                            # sales_history_forecast_mapping_result = gzip.decompress(sales_history_forecast_mapping_result)
                            sales_history_forecast_mapping_result = ZstdReadWriter.decompressZSDContent(sales_history_forecast_mapping_result)
                            sales_history_forecast_mapping_result = msgspec.json.decode(sales_history_forecast_mapping_result)
                            forecastDataFrame = pd.DataFrame(sales_history_forecast_mapping_result)
                
                    if not forecastDataFrame.empty:
                        if(forecast_type.upper() == ForecastType(2).name or forecast_type.upper() == ForecastType(4).name):
                            forecastUserHierarchyNoOfLevels = int(configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(1).name][EntitiesConfigurationKeys(2).name])
                            personelUserHierarchyKey = "ed"
                        else:
                            forecastUserHierarchyNoOfLevels = int(configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(7).name][EntitiesConfigurationKeys(8).name])
                            personelUserHierarchyKey = "es"
                        userEmail = base_util.getLoggedInUserEmailAddress(self, token)
                        
                        def userLevel(row):
                            user_level=0
                            for index in range(forecastUserHierarchyNoOfLevels, 0, -1):
                                if row[personelUserHierarchyKey + str(index)].upper() == userEmail.upper():
                                    user_level = index
                                    break
                            return user_level

                        forecastDataFrame["ul"] = forecastDataFrame.apply(userLevel, axis=1)
                        forecastDataFrame = forecastDataFrame[forecastDataFrame.ul > 0]

                        adjustment_logs = AdjustmentLog.objects.filter(forecast_header_id_fk_id__in=forecastDataFrame['fhi'],
                                                                        is_active=True, forecast_type=forecast_type)
                        read_by_set = set(str(user_level) for user_level in forecastDataFrame["ul"])
                        result = [
                            {
                                'ld': log.adjustment_log_description,
                                'ca': log.created_at,
                                'ir': bool(set(log.read_by.split(",")).intersection(read_by_set)),
                                'id': log.adjustment_log_id
                            }
                            for log in adjustment_logs
                        ]
                        #sort result based on ir in ascending order
                        result = sorted(result, key=lambda x: (not x['ir'], x['ca']), reverse=True)
                        return {"responseCode": ResponseCodes(29).name,"responseMessage": "Adjustment Log data returned Sucessfully!","status": status.HTTP_200_OK, "data": result}
                    else:
                        return {"responseCode": ResponseCodes(29).name,"responseMessage": "Adjustment Log data returned Sucessfully!","status": status.HTTP_200_OK, "data": []}
                else:
                    logger.error(f"DPAI Service: {forecast_type}AdjustmentLog-GET: SNOP Not found: tenant_id: {tenant_id} | bu_id: {bu_id} | snop_id: {snop_id}")
                    return {"responseCode":ResponseCodes(5).name,"responseMessage": "Invalid Request Body", "status": status.HTTP_400_BAD_REQUEST}
            else:
                logger.error(f"DPAI Service: {forecast_type}GET Adjustment Log: Forecast is not enabled: tenant_id: {tenant_id} | bu_id: {bu_id} | snop_id: {snop_id}")
                return {"responseCode": ResponseCodes(3).name, "responseMessage": "Forecasting is not enabled",
                    "status": status.HTTP_400_BAD_REQUEST}
        except Exception as e:
            logger.error(f"DPAI Service: {forecast_type}AdjustmentLog-GET: Logs are not avaialble: {e}")
            return {"responseCode":ResponseCodes(5).name,"responseMessage": "Adjustment Log not available!", "status": status.HTTP_400_BAD_REQUEST}
    
    def update(self, tenant_id, bu_id, snop_id, token, forecast_type, data):
        try:
            configurations = Configuration.get(tenant_id, bu_id, token)
            if ((forecast_type.upper() == ForecastType(2).name or forecast_type.upper() == ForecastType(4).name) and configurations[ForecastConfigurationKeys(2).name][ForecastConfigurationKeys(5).name]) or forecast_type.upper() == ForecastType(3).name:
                snop = Snop.objects.get(snop_id=snop_id, bu_id=bu_id, is_active=True)
                # Personal Api Integration
                personnels = Personnel.get(tenant_id, bu_id, token)
                forecast_headers = ForecastHeader.objects.filter(snop_id_fk=snop_id, is_active=True)

                if snop and personnels and forecast_headers:
                    userEmail = base_util.getLoggedInUserEmailAddress(self, token)
                    # create Personnels DataFrame
                    personnelsDataFrame = pd.DataFrame(personnels)
                    # create Forcast DataFrame
                    forecastDataFrame=pd.DataFrame.from_records(forecast_headers.values())
                    forecastDataFrame = forecastDataFrame.drop(columns=['created_at'])
                    forecastDataFrame["sku_id"] = forecastDataFrame["sku_id"].map(lambda x: str(x))
                    forecastDataFrame["node_id"] = forecastDataFrame["node_id"].map(lambda x: str(x))
                    forecastDataFrame["channel_id"] = forecastDataFrame["channel_id"].map(lambda x: str(x))

                    if (forecast_type.upper() == ForecastType(2).name or forecast_type.upper() == ForecastType(4).name):
                        forecastUserHierarchyNoOfLevels = int(configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(1).name][EntitiesConfigurationKeys(2).name])
                        personelUserHierarchyKey = EntitiesConfigurationKeys(15).name
                    else:
                        forecastUserHierarchyNoOfLevels = int(configurations[EntitiesConfigurationKeys(17).name][EntitiesConfigurationKeys(7).name][EntitiesConfigurationKeys(8).name])
                        personelUserHierarchyKey = EntitiesConfigurationKeys(16).name

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
                    # import ipdb; ipdb.set_trace()
                    adjustment_logs = AdjustmentLog.objects.filter(adjustment_log_id__in=data["adjustment_log_ids"])
                    if adjustment_logs:
                        adjustment_logs_DataFrame = pd.DataFrame.from_records(adjustment_logs.values())
                        adjustment_logs_DataFrame = pd.merge(forecastDataFrame, adjustment_logs_DataFrame, how="inner", left_on=["forecast_header_id"], right_on=["forecast_header_id_fk_id"])
                        adjustment_logs_DataFrame['read_by'] = ','.join(set(adjustment_logs_DataFrame['user_level_y'].astype(str)))
                        adjustment_logs_DataFrame['updated_at_y'] = timezone.now()
                        adjustment_logs_DataFrame['updated_by_y'] = base_util.getLoggedInUserId(token)
                        adjustment_logs_DataFrame = adjustment_logs_DataFrame[['is_active_y', 'created_at', 'updated_at_y', 'created_by_y', 'updated_by_y','adjustment_log_id', 'forecast_header_id_fk_id', 'adjustment_log_description','forecast_type', 'user_level_y', 'read_by']]
                        adjustment_logs_DataFrame = adjustment_logs_DataFrame.rename(columns={'is_active_y':'is_active', 'created_by_y':'created_by', 'updated_by_y':'updated_by', 'user_level_y':'user_level', 'updated_at_y':'updated_at'})
                        updatedData = []
                        for _, row in adjustment_logs_DataFrame.iterrows():
                            data = {
                                'is_active': row['is_active'],
                                'created_at': row['created_at'],
                                'updated_at': row['updated_at'],
                                'created_by': row['created_by'],
                                'updated_by': row['updated_by'],
                                'adjustment_log_id': row['adjustment_log_id'],
                                'forecast_header_id_fk_id': row['forecast_header_id_fk_id'],
                                'adjustment_log_description': row['adjustment_log_description'],
                                'forecast_type': row['forecast_type'],
                                'user_level': row['user_level'],
                                'read_by': row['read_by']
                            }
                            updatedData.append(data)
                            adjustment_log, _ = AdjustmentLog.objects.update_or_create(adjustment_log_id=row['adjustment_log_id'], defaults=data)
                        return {"responseCode": ResponseCodes(29).name,"responseMessage": "Adjustment Log data returned Sucessfully!","status": status.HTTP_200_OK, "data": updatedData}
                    else:
                        return {"responseCode": ResponseCodes(5).name,"responseMessage": "Invalid Request Body!","status": status.HTTP_200_OK, "data": []}
                else:
                    logger.error(f"DPAI Service: {forecast_type}AdjustmentLog-GET: SNOP Not found: tenant_id: {tenant_id} | bu_id: {bu_id} | snop_id: {snop_id}")
                    return {"responseCode":ResponseCodes(5).name,"responseMessage": "Invalid Request Body", "status": status.HTTP_400_BAD_REQUEST}
            
            else:
                logger.error(f"DPAI Service: {forecast_type}GET Adjustment Log: Forecast is not enabled: tenant_id: {tenant_id} | bu_id: {bu_id} | snop_id: {snop_id}")
                return {"responseCode": ResponseCodes(3).name, "responseMessage": "Forecasting is not enabled","status": status.HTTP_400_BAD_REQUEST}
        except Exception as e:
            logger.error(f"DPAI Service: {forecast_type}AdjustmentLog-GET: Logs are not avaialble: {e}")
            return {"responseCode":ResponseCodes(5).name,"responseMessage": "Adjustment Log not available!", "status": status.HTTP_400_BAD_REQUEST}
    
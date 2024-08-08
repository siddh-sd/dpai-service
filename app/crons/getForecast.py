from snop.models import Snop
from app.model.forecast.forecast_number import ForecastNumber
import logging
import json
logger = logging.getLogger(__name__)
import requests
from django.conf import settings
from azure.storage.blob import BlobServiceClient
from com_scai_dpai.helper.login import Login
from com_scai_dpai.helper.configuration import Configuration
from app.enum import ForecastConfigurationKeys, DataScienceGetForecastAttributes, DataScienceForecastStatus, ForecastConfigurationKeys, ForecastType, SnopConfigurationKeys, ProductAttributeNames, EntitiesConfigurationKeys, LocationAttributeNames, SalesHistoryAttributes, PlanningFrequencies
from django.utils import timezone
from django.db.models import Q
from django_tenants.utils import tenant_context, get_tenant_model
import pandas as pd
from dateutil.parser import parse
from app.model.forecast.forecast_header import ForecastHeader
from app.model.forecast.forecast_detail import ForecastDetail
from app.utils import Util
from app.helper.entities.product import Product
from app.helper.entities.location import Location
from app.helper.transactional.salesHistory import SalesHistory
from com_scai_dpai.utils import Util as base_util
import numpy as np
from django.db import transaction
from app.serializers.forecast.forecast_detail import ForecastDetailSerializer

blob_service_client = BlobServiceClient.from_connection_string(settings.BLOB_URL)

@transaction.atomic            
def getForecast():
    try:
        sysToken = Login.get_token(settings.SYS_ADMIN_EMAIL, settings.SYS_ADMIN_PASSWORD)
        for tenant in get_tenant_model().objects.all():
            with tenant_context(tenant):
                logger.info(f'GET FORECAST ALL TENANT IDs : {str(tenant.tenant_id)}')
                tenant_id = str(tenant.tenant_id)
                if (tenant_id).upper() != 'PUBLIC':
                    forecasts = ForecastNumber.objects.filter(~Q(forecast_status__in = [DataScienceForecastStatus(1).name.upper(), DataScienceForecastStatus(5).name.upper()]))
                    for forecast in forecasts:
                        try:
                            snop = Snop.objects.get(snop_id = forecast.snop_id)
                            if snop:
                                configuration_response = Configuration.get(tenant_id, snop.bu_id, sysToken)
                                logger.info(f'GET FORECAST Trying to get Forecast for {forecast.forecast_number}')
                                # API Integration
                                detail = requests.get(f"{settings.DATA_SCIENCE_URL}{forecast.forecast_number}")
                                if configuration_response and detail.status_code == 200 and detail.json()[DataScienceGetForecastAttributes(1).name] and detail.json()[DataScienceGetForecastAttributes(1).name].upper() == DataScienceForecastStatus(1).name.upper():
                                    forecast_csv_link = detail.json()[DataScienceGetForecastAttributes(2).name][DataScienceGetForecastAttributes(3).name]
                                    forecastDataFrame = pd.read_csv(forecast_csv_link)
                                    forecastUniqueDataFrame = forecastDataFrame[[DataScienceGetForecastAttributes(15).name, DataScienceGetForecastAttributes(16).name, DataScienceGetForecastAttributes(17).name]].drop_duplicates()
                                    detail_values = []
                                    approval_values = []
                                    header_values = []
                                    systemUserId = base_util.getLoggedInUserId(sysToken)
                                    currentDate = timezone.now()
                                    ForecastHeader.objects.filter(is_active = True, is_re_forecasted = True, snop_id_fk = snop).update(is_active = False)
                                    
                                    forecastUniqueDataFrame = json.loads(forecastUniqueDataFrame.to_json(orient='records',default_handler=str,date_format='iso'))
                                    forecastDataFrame = json.loads(forecastDataFrame.to_json(orient='records',default_handler=str,date_format='iso'))
                                    for key in forecastUniqueDataFrame:
                                        header_values.append(ForecastHeader(
                                            snop_id_fk=snop,
                                            sku_id=key[DataScienceGetForecastAttributes(15).name],
                                            node_id=key[DataScienceGetForecastAttributes(16).name],
                                            channel_id=key[DataScienceGetForecastAttributes(17).name],
                                            variability="X",#TODO
                                            segment="A",#TODO
                                            adi=1.32,#TODO
                                            cv=0.49,#TODO
                                            created_at=currentDate,
                                            updated_at=currentDate,
                                            created_by=systemUserId,
                                            updated_by=systemUserId,
                                            sparsity="H",#TODO,
                                            is_seasonal=False,#TODO
                                            fmr="F"#TODO
                                        ))

                                        '''Add data to ForecastApproval'''
                                        for f_type in [ForecastType(4).name.upper(), ForecastType(2).name.upper(), ForecastType(3).name.upper()]:
                                            approval_values.append({
                                                "sku_id" : key[DataScienceGetForecastAttributes(15).name],
                                                "node_id" : key[DataScienceGetForecastAttributes(16).name],
                                                "channel_id" : key[DataScienceGetForecastAttributes(17).name],
                                                "approved_till_level": 0,
                                                "created_at" : currentDate,
                                                "updated_at" : currentDate,
                                                "created_by" : systemUserId,
                                                "updated_by" : systemUserId,
                                                "forecast_type": f_type
                                            })

                                    logger.info('GET FORECAST: Forecast Header & Approval created successfully')

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
                                    forecastApprovalsDataFrame = pd.DataFrame(approval_values)
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
                                    
                                    forecastApprovalsDataFrame = pd.merge(forecastApprovalsDataFrame, forecastHeadersDataFrame, how="inner", left_on=["sku_id", "node_id", "channel_id"], right_on=["sku_id", "node_id", "channel_id"])
                                    forecastApprovalsDataFrame = forecastApprovalsDataFrame[["created_at", "updated_at", "created_by", "updated_by", "forecast_type", "approved_till_level", "forecast_header_id_fk"]]
                                    forecastApprovalsDataFrame = json.loads(forecastApprovalsDataFrame.to_json(orient='records',default_handler=str,date_format='iso'))
                                    # FORECAST APPROVAL TODO
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
                                    logger.info('CREATE FORECAST ForecastApproval data added successfully')
                                    
                                    forecast.forecast_status = DataScienceForecastStatus(1).name.upper()
                                    forecast.save()
                                else:
                                    if detail.status_code == 200 and detail.json()[DataScienceGetForecastAttributes(1).name] and detail.json()[DataScienceGetForecastAttributes(1).name].upper() == DataScienceForecastStatus(5).name.upper():
                                        forecast.forecast_status = DataScienceForecastStatus(5).name.upper()
                                        forecast.save()
                                        logger.info(f'DPAI Service: Get Forecast: Forecast failed ready for ' + f'{forecast.forecast_number}')
                                    else:
                                        logger.info(f'DPAI Service: Get Forecast: Forecast not yet ready for ' + f'{forecast.forecast_number}')
                        except Exception as e:
                            logger.error("DPAI Service Error occurred getForecast: " + f'{e}')
    except Exception as e:
        logger.error("DPAI Service Error occurred getForecast: " + f'{e}')
import time
from snop.models import Snop
from datetime import date, datetime
from app.model.forecast.forecast_number import ForecastNumber
import logging
from dateutil.relativedelta import relativedelta
logger = logging.getLogger(__name__)
import requests
from django.conf import settings
import json
import os
from azure.storage.blob import BlobServiceClient
from com_scai_dpai.helper.login import Login
from app.helper.entities.product import Product
from app.helper.entities.location import Location
from app.helper.transactional.salesHistory import SalesHistory
from com_scai_dpai.helper.configuration import Configuration
from app.helper.entities.mapping import Mapping
from snop.serializers import SnopSerializer
from app.enum import ForecastConfigurationKeys, SalesHistoryAttributes, ProductAttributeNames, LocationAttributeNames, MappingAttributes, DataScienceGetForecastAttributes, DataScienceForecastStatus
from django.utils import timezone
from django_tenants.utils import tenant_context, get_tenant_model
import pandas as pd
from com_scai_dpai.utils import Util as base_util
from app.utils import Util
from app.helper.transactional.feature import Feature
blob_service_client = BlobServiceClient.from_connection_string(settings.BLOB_URL)

def triggerForecast():
    try:
        logger.info(f'DPAI Service: Trigger Forecast')
        for tenant in get_tenant_model().objects.all():
            with tenant_context(tenant):
                # logger.info(f'Trigger Forecast ALL TENANT IDs : {str(tenant.tenant_id)}')
                tenant_id = str(tenant.tenant_id)
                if tenant_id != "Public":
                    snops = Snop.objects.filter(is_active=True, forecast_trigger_date=date.today())
                    for snop in snops:
                        forecastNumberForSnops = ForecastNumber.objects.filter(is_active=True, snop_id=snop.snop_id, forecast_status__in=[DataScienceForecastStatus(1).name, DataScienceForecastStatus(2).name, DataScienceForecastStatus(3).name, DataScienceForecastStatus(4).name])
                        if not forecastNumberForSnops:
                            logger.info(f'Trigger Forecast snop : {snop}')
                            logger.info(f'Trigger Forecast tenant_id : {tenant_id}')

                            sysToken = Login.get_token(settings.SYS_ADMIN_EMAIL, settings.SYS_ADMIN_PASSWORD)
                            '''mapping API integration'''
                            mapping_response = Mapping.get(tenant_id, snop.bu_id, sysToken)
                            '''location API integration'''
                            location_response = Location.get(tenant_id, snop.bu_id, sysToken)
                            '''Product API integration'''
                            product_response = Product.get(tenant_id, snop.bu_id, sysToken)
                            '''configuration API integration'''
                            configuration_response = Configuration.get(tenant_id, snop.bu_id, sysToken)
                            '''historicalSales API integration'''
                            logger.info(f'Trigger Forecast SALES HISTORY REQUEST START {datetime.now()}')
                            historicalSales_response = SalesHistory.get(tenant_id, snop.bu_id, sysToken,",id,channel_name")
                            logger.info(f'Trigger Forecast SALES HISTORY REQUEST END {datetime.now()}')
                            '''Features API integration'''
                            feature_response = []
                            forecastFeatures = configuration_response[ForecastConfigurationKeys(2).name][ForecastConfigurationKeys(20).name] if ForecastConfigurationKeys(20).name in configuration_response[ForecastConfigurationKeys(2).name] else []
                            if len(forecastFeatures) > 0:
                                feature_response = Feature.get(tenant_id, snop.bu_id, sysToken)
                            sales_list = []
                            product_list = []
                            location_list = []
                            mapping_list = []
                            feature_list = []
                            channel_list = []
                            salesHistoryConsiderationDate = snop.from_date - relativedelta(months=int(configuration_response[ForecastConfigurationKeys(2).name][ForecastConfigurationKeys(6).name]))
                            forecastSalesChannel = configuration_response[ForecastConfigurationKeys(2).name][ForecastConfigurationKeys(7).name]
                            forecastNodeType = configuration_response[ForecastConfigurationKeys(2).name][ForecastConfigurationKeys(8).name]
                            today_date = datetime.strptime(str(date.today()), "%Y-%m-%d").date()
                            if len(forecastSalesChannel) > 0:
                                forecastSalesChannel = forecastSalesChannel.split(",")
                            if len(forecastNodeType) > 0:
                                forecastNodeType = forecastNodeType.split(",")                 
                            
                            for product in product_response:
                                if (len(forecastSalesChannel) == 0  or (len(forecastSalesChannel) > 0 and product[ProductAttributeNames(5).name].upper() in map(str.upper, forecastSalesChannel))) and bool(product[ProductAttributeNames(9).name] and (not product[ProductAttributeNames(10).name] or datetime.strptime(product[ProductAttributeNames(10).name], "%Y-%m-%d").date() >= today_date)):
                                    product["sku_title"] = product[ProductAttributeNames(2).name]
                                    product["sku_id"] = product[ProductAttributeNames(1).name]
                                    product["like_sku_id"] = product[ProductAttributeNames(12).name]
                                    del product[ProductAttributeNames(2).name]
                                    del product[ProductAttributeNames(1).name]
                                    product_list.append(product)
                                    channel_list.append({"channel_id": product[ProductAttributeNames(6).name], "channel_name": product[ProductAttributeNames(5).name]})
                            
                            for location in location_response:
                                if (len(forecastNodeType) == 0  or (len(forecastNodeType) > 0 and location[LocationAttributeNames(3).name][LocationAttributeNames(4).name].upper() in map(str.upper, forecastNodeType)) and bool(location[LocationAttributeNames(8).name] and (not location[LocationAttributeNames(9).name] or datetime.strptime(location[LocationAttributeNames(9).name], "%Y-%m-%d").date() >= today_date))):
                                    location["node_id"] = location[LocationAttributeNames(1).name]
                                    location["node_title"] = location[LocationAttributeNames(2).name]
                                    location["node_status"] = location[LocationAttributeNames(8).name]
                                    del location[LocationAttributeNames(7).name]
                                    del location[LocationAttributeNames(6).name]
                                    del location[LocationAttributeNames(3).name]
                                    del location[LocationAttributeNames(1).name]
                                    del location[LocationAttributeNames(2).name]
                                    del location[LocationAttributeNames(8).name]
                                    location_list.append(location)
                            
                            for historicalSale in historicalSales_response:
                                if product_list and location_list and datetime.strptime(historicalSale[SalesHistoryAttributes(3).name], "%Y-%m-%d").date() >= salesHistoryConsiderationDate and (len(forecastSalesChannel) == 0  or (len(forecastSalesChannel) > 0 and historicalSale[SalesHistoryAttributes(9).name].upper() in map(str.upper, forecastSalesChannel))):
                                    # isRequiredRecord = False
                                    # for prod in product_list:
                                    #     if (prod["sku_id"] == historicalSale[SalesHistoryAttributes(11).name] or prod["like_sku_id"] == historicalSale[SalesHistoryAttributes(11).name]) and prod[ProductAttributeNames(6).name] == historicalSale[SalesHistoryAttributes(10).name]:
                                    #         isRequiredRecord = True
                                    #         break
                                    # if isRequiredRecord:
                                    #     isRequiredRecord = False
                                    #     for loc in location_list:
                                    #         if loc["node_id"] == historicalSale[SalesHistoryAttributes(12).name]:
                                    #             isRequiredRecord = True
                                    #             break
                                    # if isRequiredRecord:
                                    sales_list.append({"id": historicalSale[SalesHistoryAttributes(7).name], "date": historicalSale[SalesHistoryAttributes(3).name], "sku_id": historicalSale[SalesHistoryAttributes(11).name], "node_id": historicalSale[SalesHistoryAttributes(12).name], "actual_sales_volume": historicalSale[SalesHistoryAttributes(5).name], "actual_sales_value": historicalSale[SalesHistoryAttributes(6).name], "channel_id": historicalSale[SalesHistoryAttributes(10).name], "channel_name": historicalSale[SalesHistoryAttributes(9).name]})
                                    channel_list.append({"channel_id": historicalSale[SalesHistoryAttributes(10).name], "channel_name": historicalSale[SalesHistoryAttributes(9).name]})
                            
                            for mapping in mapping_response:
                                if product_list and location_list and bool(mapping[MappingAttributes(6).name]) and (not mapping[MappingAttributes(7).name] or datetime.strptime(mapping[MappingAttributes(7).name], "%Y-%m-%d").date() >= today_date):
                                    mapping["sku_id"] = mapping[MappingAttributes(1).name][MappingAttributes(3).name]
                                    mapping["node_id"] = mapping[MappingAttributes(2).name][MappingAttributes(3).name]
                                    mapping["channel_id"] = mapping[MappingAttributes(4).name][MappingAttributes(5).name]
                                    # isRequiredRecord = False
                                    # for prod in product_list:
                                    #     if (prod["sku_id"] == mapping["sku_id"] or prod["like_sku_id"] == mapping["sku_id"]) and prod[ProductAttributeNames(6).name] == mapping["channel_id"]:
                                    #         isRequiredRecord = True
                                    #         break
                                    # if isRequiredRecord:
                                    #     isRequiredRecord = False
                                    #     for loc in location_list:
                                    #         if loc["node_id"] == mapping["node_id"]:
                                    #             isRequiredRecord = True
                                    #             break
                                    # if isRequiredRecord:
                                    del mapping[MappingAttributes(2).name]
                                    del mapping[MappingAttributes(1).name]
                                    del mapping[MappingAttributes(4).name]
                                    mapping_list.append(mapping)
                            
                            if feature_response:
                                for feature in feature_response:
                                    featureItem = {}
                                    featureItem["sku_id"] = feature["sku_id"]
                                    featureItem["node_id"] = feature["node_id"]
                                    featureItem["channel_id"] = feature["channel_id"]
                                    featureItem["date"] = feature["date"]
                                    for forecastFeature in forecastFeatures:
                                        featureItem[forecastFeature] = feature[forecastFeature]
                                        feature_list.append(featureItem)

                            channel_list = pd.DataFrame(channel_list).drop_duplicates().to_dict('records')

                            if snop and configuration_response and product_list and location_list and mapping_list and sales_list and channel_list:
                                request_body = {
                                    "configuration": configuration_response,
                                    "webhook": settings.DPAI_API_URL + settings.STATISTICAL_FORECAST_BASE_URL,
                                    "tenant_id": tenant_id,
                                    "bu_id": snop.bu_id,
                                    "data": {
                                        "snop": SnopSerializer(snop).data,
                                        "product": Util.createUploadCSV(f"Product_{snop.snop_id}_{timezone.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv", product_list, settings.BLOB_FORECAST_MASTER_FILES_CONTAINER),
                                        "location": Util.createUploadCSV(f"Location_{snop.snop_id}_{timezone.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv", location_list, settings.BLOB_FORECAST_MASTER_FILES_CONTAINER),
                                        "mapping": Util.createUploadCSV(f"Mapping_{snop.snop_id}_{timezone.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv", mapping_list, settings.BLOB_FORECAST_MASTER_FILES_CONTAINER),
                                        "historicalSales": Util.createUploadCSV(f"HistoricalSales_{snop.snop_id}_{timezone.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv", sales_list, settings.BLOB_FORECAST_MASTER_FILES_CONTAINER),
                                        "channel": Util.createUploadCSV(f"Channel_{snop.snop_id}_{timezone.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv", channel_list, settings.BLOB_FORECAST_MASTER_FILES_CONTAINER),
                                        "externalfactors": Util.createUploadCSV(f"ExternalFactors_{snop.snop_id}_{timezone.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv", feature_list, settings.BLOB_FORECAST_MASTER_FILES_CONTAINER)
                                    }
                                }
                                
                                logger.info(f'DS API REQUEST {datetime.now()}')
                                # API Integration
                                response = requests.post(settings.DATA_SCIENCE_URL, headers={'Content-Type': 'application/json'}, data=json.dumps(request_body))
                                if response.status_code == 201:
                                    forecast_data = {"forecast_number": response.json()[DataScienceGetForecastAttributes(7).name],
                                        "forecast_status": response.json()[DataScienceGetForecastAttributes(1).name],
                                        "created_at": timezone.now(), "updated_at": timezone.now(),
                                        "created_by": base_util.getLoggedInUserId(sysToken),
                                        "updated_by": base_util.getLoggedInUserId(sysToken),
                                        "snop_id": snop.snop_id}

                                    ForecastNumber.objects.create(**forecast_data)
                                else:
                                    logger.error("DPAI Service Trigger Forecast Error occurred: Forecast not triggered", json.dumps(request_body))
                            else:
                                logger.error("DPAI Service Trigger Forecast Error occurred: Master data not available")
                        else:
                            logger.info(f"DPAI Service Trigger Forecast Forecast Already triggered snop: {snop}")

        logger.debug(f'DPAI Service: Trigger Forecast Completed')
    except Exception as e:
        logger.error("DPAI Service Error occurred triggerForecast: " + f'{e}')

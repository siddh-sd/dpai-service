import gzip
import json
from app.enum import ForecastType, LocationAttributeNames, PersonnelAttributeNames, EntitiesConfigurationKeys, AdjustmentOperations, ProductAttributeNames, Variability, Segment, DemandClassification, PlanningFrequencies
import logging
from app.helper.utils.ZstdReadWriter import ZstdReadWriter
from app.serializers.forecast.forecast_header import ForecastHeaderSerializer
from dateutil.relativedelta import relativedelta
import numpy as np
from datetime import datetime, timedelta, date
logger = logging.getLogger(__name__)
import pandas as pd
import os
from django.conf import settings
from azure.storage.blob import BlobServiceClient
blob_service_client = BlobServiceClient.from_connection_string(settings.BLOB_URL)

class Util:

    @staticmethod
    def isForecastPermitted(self, personnel, forecastUserHierarchyNoOfLevels, userEmail, personnelUserLevelKey):
        try:
            user_level = 0
            if personnel and forecastUserHierarchyNoOfLevels > 0 and userEmail and personnelUserLevelKey:
                for index in range(forecastUserHierarchyNoOfLevels, 0, -1):  # high to low
                    if personnel[personnelUserLevelKey + str(index)].upper() == userEmail.upper():
                        user_level = index
                        break
                return user_level
            else:
                return user_level
        except Exception as e:
            logger.error(f"DPAI Service: isForecastPermitted Exception: {e}")
            return 0

    @staticmethod
    def isForecastUpdatePermitted(self, forecasts, personnels, userEmail, totalLevels, personnelUserLevelKey,
                                  forecastType, forecastApprovals):
        try:
            if forecasts and forecasts != [] and personnels and userEmail and totalLevels and personnelUserLevelKey and forecastType:
                forecastData = ForecastHeaderSerializer(forecasts, many=True)
                forecastData = forecastData.data
                isUserAccess = False
                for forecast in forecastData:
                    sku_id = str(forecast["sku_id"])
                    node_id = str(forecast["node_id"])
                    channel_id = str(forecast["channel_id"])
                    personnel = list(filter(
                        lambda dct: dct[PersonnelAttributeNames(3).name] == sku_id and dct[
                            PersonnelAttributeNames(5).name] == node_id and dct[PersonnelAttributeNames(7).name] == channel_id, personnels))
                    if personnel != []:
                        '''user level'''
                        for level in range(totalLevels, 0, -1):  # high to low
                            if personnel[0].get(personnelUserLevelKey + str(level)).upper() == userEmail.upper():
                                approval = forecastApprovals.filter(
                                    forecast_header_id_fk=forecast["forecast_header_id"]).first()
                                if not (approval and approval.approved_till_level < level):
                                    return False
                                isUserAccess = True
                                break
                    if not isUserAccess:
                        return False
                return True
            else:
                return False
        except Exception as e:
            logger.error(f"DPAI Service: isForecastUpdatePermitted Exception: {e}")
            return False

    # isSnopValidationRequired need snop to be passed for this to work
    # isForecastUpdatePermissionValidationRequired need snop & forecastids in comma separated array, personnels, userEmail, totalLevels, personnelUserLevelKey, forecastType, forecasts to be passed for this to work
    # isPreviousForecastClosureValidationRequired need previousForecastApprovals & previousForecastUserTotalLevels to be passed for this to work
    @staticmethod
    def ValidateRequest(self, isSnopValidationRequired, isForecastUpdatePermissionValidationRequired,
                        isPreviousForecastClosureValidationRequired, snop, forecasts, personnels, userEmail,
                        totalLevels, personnelUserLevelKey, forecastType, forecastApprovals, previousForecastApprovals,
                        previousForecastUserTotalLevels):
        try:
            if isSnopValidationRequired and not Util.isSnopActive(self, snop):
                return False

            if isPreviousForecastClosureValidationRequired and previousForecastApprovals:
                for index in range(0, len(previousForecastApprovals)):
                    if previousForecastApprovals[index].approved_till_level != previousForecastUserTotalLevels:
                        return False

            if isForecastUpdatePermissionValidationRequired and not Util.isForecastUpdatePermitted(self, forecasts,
                                                                                                   personnels,
                                                                                                   userEmail,
                                                                                                   totalLevels,
                                                                                                   personnelUserLevelKey,
                                                                                                   forecastType,
                                                                                                   forecastApprovals):
                return False

            return True
        except Exception as e:
            logger.error(f"DPAI Service: ValidateRequest Exception: {e}")
            return False

    @staticmethod
    def isVolumeValid(self, data, planningHorizon):
        logger.debug(f"isVolumeValid: {data}")
        try:
            if planningHorizon == len(data):
                for record in data:
                    if record['volume'] >= 0 and type(record['volume']) == int:
                        logger.debug(f"Volume validation passed for data : {data}")
                    else:
                        logger.debug(f"Volume validation failed for data : {data}")
                        return False
                return True
            else:
                return False
        except Exception as e:
            logger.error(f"DPAI Service: isVolumeValid Exception: {e}")
            return False

    @staticmethod
    def isSnopActive(self, snop):
        try:
            if snop:
                current_date = date.today()
                current_date = datetime.strptime(str(current_date), "%Y-%m-%d").date()
                from_date = datetime.strptime(str(snop.from_date), "%Y-%m-%d").date()
                snop_date = datetime.strptime(str(snop.snop_date), "%Y-%m-%d").date()
                if current_date < from_date and current_date <= snop_date:
                    return True
                else:
                    return False
            else:
                return False
        except Exception as e:
            logger.error({"DPAI Service:  Error at isSnopActive": f"{e}"})
            return False

    @staticmethod
    def castForecastType(forecast_type):
        try:
            forecastTypes = {ForecastType(index).name: index for index in range(1, len(ForecastType) + 1)}
            if forecast_type.upper() in forecastTypes.keys():
                return forecastTypes[forecast_type.upper()]
            else:
                return 0
        except Exception as e:
            logger.error({"DPAI Service:  Error at castForecastType": f"{e}"})
            return 0

    @staticmethod
    def getVolume(self, volume, operation, adjustment):
        try:
            if operation:
                if AdjustmentOperations(1).name == operation.upper():
                    return int(volume) + int(round((volume * int(adjustment)) / 100, 0))
                elif AdjustmentOperations(2).name == operation.upper():
                    return int(volume) - int(round((volume * int(adjustment)) / 100, 0))
                elif AdjustmentOperations(3).name == operation.upper():
                    return int(volume) + int(adjustment)
                elif AdjustmentOperations(4).name == operation.upper():
                    updatedVolume = int(volume) - int(adjustment)
                    if updatedVolume < 0:
                        return 0
                    else:
                        return updatedVolume
            return 0
        except Exception as e:
            logger.error({"DPAI Service:  Error at getVolume": f"{e}"})
            return 0

    #Demand Classification and Variability
    @staticmethod
    def getDemandClassificationAndVariability(sales=None, sparsityHigh=0, sparsityMedium=0):
        try:
            adi = 0
            variability = ""
            coefficientVariation = 0

            if sales and len(sales) > 0:
                sales = np.array(sales)
                relevantSales = sales[sales>0]
                adi = len(sales)/len(relevantSales) if len(relevantSales) != 0 else 0
                standardDeviation = np.std(relevantSales)
                average = np.mean(relevantSales)
                coefficientVariation = (standardDeviation/average) if average != 0 else 0

                if coefficientVariation >= sparsityHigh:
                    variability = Variability(1).name
                elif coefficientVariation < sparsityHigh and coefficientVariation >= sparsityMedium:
                    variability = Variability(2).name
                else:
                    variability = Variability(3).name
   
            return adi, coefficientVariation**2, variability
        except Exception as e:
            logger.error("DPAI Service Error occurred getDemandClassification: " + f'{e}')
            return 0, 0, ""

    #Segment
    @staticmethod
    def getSegment(segmentValue=0, definitionClassificationA=0, definitionClassificationB=0):
        try:
            segment = ""
            if segmentValue <= definitionClassificationA:
                segment = Segment(1).name
            elif segmentValue <= definitionClassificationB:
                segment = Segment(2).name
            else:
                segment = Segment(3).name

            return segment

        except Exception as e:
            logger.error("DPAI Service Error occurred getSegment: " + f'{e}')
            return ""

    @staticmethod
    def getSalesHistoryRange(visible_sales_history, snop, planning_cycle_frequency, horizon, isForecastMonthToBeIncluded):
        try:
            sales_history_range = []
            for rangeIndex in range(1, visible_sales_history+2):
                if planning_cycle_frequency.upper() == PlanningFrequencies(1).name.upper():
                    sales_history_range.append(snop.from_date - timedelta(days=rangeIndex))
                elif planning_cycle_frequency.upper() == PlanningFrequencies(2).name.upper():
                    sales_history_range.append(snop.from_date - timedelta(days=rangeIndex*15))
                elif planning_cycle_frequency.upper() == PlanningFrequencies(3).name.upper():
                    sales_history_range.append(snop.from_date - timedelta(days=rangeIndex*7))
                elif planning_cycle_frequency.upper() == PlanningFrequencies(4).name.upper():
                    sales_history_range.append(snop.from_date - relativedelta(months=rangeIndex))
                elif planning_cycle_frequency.upper() == PlanningFrequencies(5).name.upper():
                    sales_history_range.append(snop.from_date - relativedelta(months=rangeIndex*3))

            if isForecastMonthToBeIncluded:
                for rangeIndex in range(0, horizon):
                    if planning_cycle_frequency.upper() == PlanningFrequencies(1).name.upper():
                        sales_history_range.append(snop.from_date - timedelta(days=rangeIndex))
                    elif planning_cycle_frequency.upper() == PlanningFrequencies(2).name.upper():
                        sales_history_range.append(snop.from_date - timedelta(days=rangeIndex*15))
                    elif planning_cycle_frequency.upper() == PlanningFrequencies(3).name.upper():
                        sales_history_range.append(snop.from_date - timedelta(days=rangeIndex*7))
                    elif planning_cycle_frequency.upper() == PlanningFrequencies(4).name.upper():
                        sales_history_range.append(snop.from_date + relativedelta(months=rangeIndex))
                    elif planning_cycle_frequency.upper() == PlanningFrequencies(5).name.upper():
                        sales_history_range.append(snop.from_date - relativedelta(months=rangeIndex*3))

            return sales_history_range
        except Exception as e:
            logger.error(f"DPAI Service: getSalesHistoryRange: Sales History Create Data range with error: {e}")
            return []

    @staticmethod
    def createFilterResponse(data, hierarchy_no_of_levels, forecastLevel, hierarchyKey, hierarchyValue, attribute_name_key, atribute_name_value, isAbbreviated = True):
        try:
            response = {}
            verifyAdd = {}
            logger.debug(f'Product hierarchy number of levels is {hierarchy_no_of_levels} and product forecast level is {forecastLevel}')
            forecast_level = 1 if str(forecastLevel).upper() == 'BASE' else int(forecastLevel)
            '''preparing response payload'''
            '''creating keys for hierarchy'''
            for index in range(hierarchy_no_of_levels, forecast_level-1, -1):
                hierarchy_key = hierarchyKey + str(index)
                response[hierarchy_key] = []
                verifyAdd[hierarchy_key] = []
            if forecastLevel.upper() == 'BASE':
                response[hierarchyKey + '0'] = []
                verifyAdd[hierarchyKey + '0'] = []

            for item in data:
                for index in range(hierarchy_no_of_levels, forecast_level-1, -1):
                    hierarchy_key = hierarchyKey + str(index)
                    hierarchy_value = hierarchyValue + str(index)
                    if item[hierarchy_value].title() not in verifyAdd[hierarchy_key]:
                        parent = ""
                        if index != hierarchy_no_of_levels:
                            parent = item[hierarchyValue + str(index+1)].title()
                        verifyAdd[hierarchy_key].append(item[hierarchy_value].title())
                        if isAbbreviated:
                            response[hierarchy_key].append({"lb":item[hierarchy_key].title(), "v":item[hierarchy_value].title(), "pt":parent})
                        else:
                            response[hierarchy_key].append({"label":item[hierarchy_key].title(), "value":item[hierarchy_value].title(), "parent":parent})
                if forecastLevel.upper() == 'BASE' and item[attribute_name_key] not in verifyAdd[hierarchyKey + '0']:
                    verifyAdd[hierarchyKey + '0'].append(item[attribute_name_key])
                    if isAbbreviated:
                        response[hierarchyKey + '0'].append({"lb":item[atribute_name_value].title(), "v":item[attribute_name_key], "pt":item[hierarchyKey + '1'].title()})
                    else:
                        response[hierarchyKey + '0'].append({"label":item[atribute_name_value].title(), "value":item[attribute_name_key], "parent":item[hierarchyKey + '1'].title()})
            return response
        except Exception as e:
            logger.error(f"DPAI Service: Util: createFilterResponse: Forecast Filter Data create with error: {e}")
            return {}

    @staticmethod
    def createUploadCSV(filePath, jsonList, blobContainer):
        try:
            blob_Url = ""
            csvDataFrame = pd.DataFrame(jsonList)
            csvDataFrame.to_csv(filePath, index=False)
            if (os.path.exists(filePath) and os.path.isfile(filePath)):
                blob_client = blob_service_client.get_blob_client(
                    container=blobContainer,
                    blob=filePath)
                with open(file=filePath, mode="rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
                blob_Url = blob_client.url
                os.remove(filePath)

            return blob_Url
        except Exception as e:
            logger.error(f'DPAI Service Error occurred createUploadCSV: {str(e)}')
            return blob_Url

    @staticmethod
    def createUploadXLSX(filePath, jsonList, blobContainer):
        try:
            blob_Url = ""
            xlsxDataFrame = pd.DataFrame(jsonList)
            xlsxDataFrame.to_excel(filePath, index=False)
            if (os.path.exists(filePath) and os.path.isfile(filePath)):
                blob_client = blob_service_client.get_blob_client(
                    container=blobContainer,
                    blob=filePath)
                with open(file=filePath, mode="rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
                blob_Url = blob_client.url
                os.remove(filePath)

            return blob_Url
        except Exception as e:
            logger.error(f'DPAI Service Error occurred createUploadXLSX: {str(e)}')
            return blob_Url

    @staticmethod
    def createUploadJson(filePath, jsonList, blobContainer):
        try:
            blob_Url = ""
            with open(filePath, "w") as filterFile:
                json.dump(jsonList, filterFile)
            if (os.path.exists(filePath) and os.path.isfile(filePath)):
                blob_client = blob_service_client.get_blob_client(
                    container=blobContainer,
                    blob=filePath)
                with open(file=filePath, mode="rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
                blob_Url = blob_client.url
                os.remove(filePath)

            return blob_Url
        except Exception as e:
            logger.error(f'DPAI Service Error occurred createUploadJSON: {str(e)}')
            return blob_Url

    @staticmethod
    def GetBlobFilePath(file_name):
        try:
            file_path = blob_service_client.get_blob_client(container=settings.BLOB_MASTER_FILES_CONTAINER, blob=file_name)
            return file_path.primary_endpoint
        except Exception as e:
            logger.error(f'DPAI Service Error occurred GetBlobFIlePath: {str(e)}')
            return e

    @staticmethod
    def createUploadGzip(filePath, jsonList, blobContainer):
        try:
            blob_Url = ""
            # with gzip.open(filePath, "w") as filterFile:
            #     filterFile.write(json.dumps(jsonList).encode('utf-8'))
            ZstdReadWriter.createZSDFile(filePath, jsonList)
            if (os.path.exists(filePath) and os.path.isfile(filePath)):
                blob_client = blob_service_client.get_blob_client(
                    container=blobContainer,
                    blob=filePath)
                with open(file=filePath, mode="rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
                blob_Url = blob_client.url
                os.remove(filePath)

            return blob_Url
        except Exception as e:
            logger.error(f'DPAI Service Error occurred createUploadJSON: {str(e)}')
            return blob_Url

    @staticmethod
    def getNFidelityDates(planning_frequency, fromDate):
        try:
            n1_snop_date = False
            n3_snop_date = False
            if planning_frequency.upper() == PlanningFrequencies(1).name.upper():
                n1_snop_date = fromDate - timedelta(days=1)
                n3_snop_date = fromDate - timedelta(days=3)

            elif planning_frequency.upper() == PlanningFrequencies(3).name.upper():
                n1_snop_date = fromDate - timedelta(days=7)
                n3_snop_date = fromDate - timedelta(days=21)

            elif planning_frequency.upper() == PlanningFrequencies(2).name.upper():
                n1_snop_date = fromDate - timedelta(days=14)
                n3_snop_date = fromDate - timedelta(days=42)

            elif planning_frequency.upper() == PlanningFrequencies(4).name.upper():
                n1_snop_date = fromDate - relativedelta(months=1)
                n3_snop_date = fromDate - relativedelta(months=3)

            elif planning_frequency.upper() == PlanningFrequencies(5).name.upper():
                n1_snop_date = fromDate - relativedelta(months=3)
                n3_snop_date = fromDate - relativedelta(months=9)

            return n1_snop_date, n3_snop_date
        except Exception as e:
            logger.error(f"DPAI Service: getNFidelityDates: getNFidelityDates with error: {e}")
            return False, False
    
    @staticmethod
    def bytes_to_megabytes(bytes_value):
        return bytes_value / (1024 * 1024)
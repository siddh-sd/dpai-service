import gzip
import json
import mgzip
import snappy
from django.http import HttpResponse
import msgspec
from rest_framework import status
from rest_framework.views import APIView
from app.enum import SnopStatus, SnopConfigurationKeys, EntitiesConfigurationKeys, ForecastConfigurationKeys, \
    SegmentationType, ForecastType, ForecastStatus, ResponseCodes
import logging
from app.helper.utils.NumpyArrayEncoder import NumpyArrayEncoder
from app.utils import Util
from com_scai_dpai.utils import Util as base_util
from app.services.forecast.forecast import ForecastService
from com_scai_dpai.decorators import user_access_permission
from django.utils.decorators import method_decorator
from datetime import datetime
from app.services.forecast.memory import get_memory_usage
logger = logging.getLogger(__name__)
import tracemalloc

class Forecast(APIView):

    @method_decorator(user_access_permission(permissions=['FORECAST_UPDATE', 'COLLABORATION_UPDATE']))
    def put(self, request, forecast_type):
        try:
            forecastType = Util.castForecastType(forecast_type)
            if forecastType == 0:
                logger.error(f"DPAI Service: {forecast_type}Forecast-PUT: Invalid ForecastType: tenant_id: {self.request.query_params.get('tenant_id')} | bu_id: {self.request.query_params.get('bu_id')} | snop_id: {self.request.query_params.get('snop_id')}")
                return base_util.createFailureResponse(self, ResponseCodes(5).name, 'Invalid Request', status.HTTP_404_NOT_FOUND)
            check_tenant_buid_snop_id = base_util.validateRequestParams(self, request)
            if check_tenant_buid_snop_id is False:
                logger.error(f"DPAI Service: {forecast_type}Forecast-PUT: UNAUTHORIZED: tenant_id: {self.request.query_params.get('tenant_id')} | bu_id: {self.request.query_params.get('bu_id')} | snop_id: {self.request.query_params.get('snop_id')}")
                return base_util.createFailureResponse(self, ResponseCodes(2).name, 'Unauthorized Request', status.HTTP_401_UNAUTHORIZED)
            logger.debug(
                f"tenant_id: {request.GET.get('tenant_id')} | bu_id: {request.GET.get('bu_id')} | snop_id: {request.GET.get('snop_id')}")
            result = ForecastService.save(self, self.request.query_params.get('tenant_id'), self.request.query_params.get('bu_id'), self.request.query_params.get('snop_id'), request.data, str.replace(str(request.META.get('HTTP_AUTHORIZATION')), 'Bearer ', ''), ForecastType(forecastType).name)
            if result['status'] != 200:
                return base_util.createFailureResponse(self, result['responseCode'], result['responseMessage'], status.HTTP_400_BAD_REQUEST)
            else:
                return base_util.createSuccessResponse(self, result['data'], result['responseCode'], result['responseMessage'], status.HTTP_200_OK)
        except Exception as e:
            logger.error(f"DPAI Service: {forecast_type}Forecast-PUT: Forecast save updated rejected with error: {e}")
            return base_util.createFailureResponse(self, ResponseCodes(5).name, 'Forecast save rejected!', status.HTTP_400_BAD_REQUEST)

    @method_decorator(user_access_permission(permissions=['FORECAST_VIEW', 'COLLABORATION_VIEW']))
    def get(self, request, forecast_type):
        try:
            forecastType = Util.castForecastType(forecast_type)
            if forecastType == 0:
                logger.error(f"DPAI Service: {forecast_type}Forecast-GET: Invalid ForecastType: tenant_id: {self.request.query_params.get('tenant_id')} | bu_id: {self.request.query_params.get('bu_id')} | snop_id: {self.request.query_params.get('snop_id')}")
                return base_util.createFailureGZipResponse(self, ResponseCodes(5).name, 'Invalid Request', status.HTTP_404_NOT_FOUND)
            check_tenant_buid_snop_id = base_util.validateRequestParams(self, request)
            if check_tenant_buid_snop_id is False:
                logger.error(f"DPAI Service: {forecast_type}Forecast-GET: UNAUTHORIZED: tenant_id: {request.GET.get('tenant_id')} | bu_id: {request.GET.get('bu_id')} | snop_id: {request.GET.get('snop_id')}")
                return base_util.createFailureGZipResponse(self, ResponseCodes(2).name, 'Unauthorized Request', status.HTTP_401_UNAUTHORIZED)
            logger.debug(
                f"tenant_id: {request.GET.get('tenant_id')} | bu_id: {request.GET.get('bu_id')} | snop_id: {request.GET.get('snop_id')}")
            
            result = ForecastService.get(self, self.request.query_params.get('tenant_id'), self.request.query_params.get('bu_id'), self.request.query_params.get('snop_id'), str.replace(str(request.META.get('HTTP_AUTHORIZATION')), 'Bearer ', ''), ForecastType(forecastType).name)
            
            if result['status'] != 200:
                return base_util.createFailureGZipResponse(self, result['responseCode'], result['responseMessage'], status.HTTP_400_BAD_REQUEST) 
            else:
                response = base_util.createSuccessGZipResponse(result['data'], result['responseCode'], result['responseMessage'], status.HTTP_200_OK)
                return response
        except Exception as e:
            logger.error(f"DPAI Service: {forecast_type}Forecast-GET: Forecast get rejected with error: {e}")
            return base_util.createFailureGZipResponse(self, ResponseCodes(5).name, 'Forecast get rejected!', status.HTTP_400_BAD_REQUEST)

    def post(self, request, forecast_type):
        try:
            forecastType = Util.castForecastType(forecast_type)
            if forecastType == 0:
                logger.error(f"DPAI Service: {forecast_type}Forecast-POST: Invalid ForecastType: tenant_id: {self.request.query_params.get('tenant_id')} | bu_id: {self.request.query_params.get('bu_id')}")
                return base_util.createFailureResponse(self, ResponseCodes(5).name, 'Invalid Request', status.HTTP_404_NOT_FOUND)
            check_tenant_buid_snop_id = base_util.validateRequestParams(self, request)
            if check_tenant_buid_snop_id is False:
                logger.error(f"DPAI Service: {forecast_type}Forecast-POST: UNAUTHORIZED: tenant_id: {request.GET.get('tenant_id')} | bu_id: {request.GET.get('bu_id')}")
                return base_util.createFailureResponse(self, ResponseCodes(2).name, 'Unauthorized Request', status.HTTP_401_UNAUTHORIZED)
            logger.debug(
                f"tenant_id: {request.GET.get('tenant_id')} | bu_id: {request.GET.get('bu_id')}")
            
            result = ForecastService.create(self, self.request.query_params.get('tenant_id'), self.request.query_params.get('bu_id'), request.data)
            if result['status'] != 200:
                return base_util.createFailureResponse(self, result['responseCode'], result['responseMessage'], status.HTTP_400_BAD_REQUEST)
            else:
                return base_util.createSuccessResponse(self, result['data'], result['responseCode'], result['responseMessage'], status.HTTP_200_OK)
        except Exception as e:
            logger.error(f"DPAI Service: {forecast_type}Forecast-POST: Forecast create rejected with error: {e}")
            return base_util.createFailureResponse(self, ResponseCodes(5).name, 'Forecast create rejected!', status.HTTP_400_BAD_REQUEST)

class ForecastXLSX(APIView):
    @method_decorator(user_access_permission(permissions=['FORECAST_VIEW', 'COLLABORATION_VIEW']))
    def get(self, request, forecast_type):
        try:
            forecastType = Util.castForecastType(forecast_type)
            if forecastType == 0:
                logger.error(f"DPAI Service: {forecast_type}Forecast-GETXLSX: Invalid ForecastType: tenant_id: {self.request.query_params.get('tenant_id')} | bu_id: {self.request.query_params.get('bu_id')} | snop_id: {self.request.query_params.get('snop_id')}")
                return base_util.createFailureGZipResponse(self, ResponseCodes(5).name, 'Invalid Request', status.HTTP_404_NOT_FOUND)
            check_tenant_buid_snop_id = base_util.validateRequestParams(self, request)
            if check_tenant_buid_snop_id is False:
                logger.error(f"DPAI Service: {forecast_type}Forecast-GETXLSX: UNAUTHORIZED: tenant_id: {request.GET.get('tenant_id')} | bu_id: {request.GET.get('bu_id')} | snop_id: {request.GET.get('snop_id')}")
                return base_util.createFailureGZipResponse(self, ResponseCodes(2).name, 'Unauthorized Request', status.HTTP_401_UNAUTHORIZED)
            logger.debug(
                f"tenant_id: {request.GET.get('tenant_id')} | bu_id: {request.GET.get('bu_id')} | snop_id: {request.GET.get('snop_id')}")
            
            result = ForecastService.getCSV(self, self.request.query_params.get('tenant_id'), self.request.query_params.get('bu_id'), self.request.query_params.get('snop_id'), str.replace(str(request.META.get('HTTP_AUTHORIZATION')), 'Bearer ', ''), ForecastType(forecastType).name)
            if "status" in result:
                return base_util.createFailureGZipResponse(self, result['responseCode'], result['responseMessage'],
                                                           status=result['status'])
            else:
                return result
        except Exception as e:
            logger.error(f"DPAI Service: {forecast_type}Forecast-GETXLSX: Forecast get rejected with error: {e}")
            return base_util.createFailureGZipResponse(self, ResponseCodes(5).name, 'Forecast get xlsx rejected!', status.HTTP_400_BAD_REQUEST)

class ForecastFilter(APIView):
    @method_decorator(user_access_permission(permissions=['FORECAST_VIEW', 'COLLABORATION_VIEW']))
    def get(self, request):
        try:
            check_tenant_buid_snop_id = base_util.validateRequestParams(self, request)
            if check_tenant_buid_snop_id is False:
                logger.error(f"DPAI Service: Forecast-GET FILTER: UNAUTHORIZED: tenant_id: {request.GET.get('tenant_id')} | bu_id: {request.GET.get('bu_id')} | snop_id: {request.GET.get('snop_id')}")
                return base_util.createFailureResponse(self, ResponseCodes(2).name, 'Unauthorized Request', status.HTTP_401_UNAUTHORIZED)
            logger.debug(
                f"tenant_id: {request.GET.get('tenant_id')} | bu_id: {request.GET.get('bu_id')} | snop_id: {request.GET.get('snop_id')}")
            
            result = ForecastService.getFilter(self, self.request.query_params.get('tenant_id'), self.request.query_params.get('bu_id'), self.request.query_params.get('snop_id'), str.replace(str(request.META.get('HTTP_AUTHORIZATION')), 'Bearer ', ''))
            if result['status'] != 200:
                return base_util.createFailureResponse(self, result['responseCode'], result['responseMessage'], status.HTTP_400_BAD_REQUEST) 
            else:
                response = base_util.createSuccessResponse(self, result['data'], result['responseCode'], result['responseMessage'], status.HTTP_200_OK)
                return response
        except Exception as e:
            logger.error(f"DPAI Service: Forecast-GET FILTER: Forecast get filter rejected with error: {e}")
            return base_util.createFailureResponse(self, ResponseCodes(5).name, 'Forecast get rejected!', status.HTTP_400_BAD_REQUEST)

class ForecastNetwork(APIView):
    @method_decorator(user_access_permission(permissions=['FORECAST_VIEW', 'COLLABORATION_VIEW']))
    def get(self, request):
        try:
            check_tenant_buid_snop_id = base_util.validateRequestParams(self, request)
            if check_tenant_buid_snop_id is False:
                logger.error(f"DPAI Service: Forecast-GET NETWORK: UNAUTHORIZED: tenant_id: {request.GET.get('tenant_id')} | bu_id: {request.GET.get('bu_id')} | snop_id: {request.GET.get('snop_id')}")
                return base_util.createFailureResponse(self, ResponseCodes(2).name, 'Unauthorized Request', status.HTTP_401_UNAUTHORIZED)
            logger.debug(
                f"tenant_id: {request.GET.get('tenant_id')} | bu_id: {request.GET.get('bu_id')} | snop_id: {request.GET.get('snop_id')}")
            
            result = ForecastService.getNetwork(self, self.request.query_params.get('tenant_id'), self.request.query_params.get('bu_id'), self.request.query_params.get('snop_id'), str.replace(str(request.META.get('HTTP_AUTHORIZATION')), 'Bearer ', ''))
            if result['status'] != 200:
                return base_util.createFailureResponse(self, result['responseCode'], result['responseMessage'], status.HTTP_400_BAD_REQUEST) 
            else:
                response = base_util.createSuccessResponse(self, result['data'], result['responseCode'], result['responseMessage'], status.HTTP_200_OK)
                return response
        except Exception as e:
            logger.error(f"DPAI Service: Forecast-GET NETWORK: Forecast get network rejected with error: {e}")
            return base_util.createFailureResponse(self, ResponseCodes(5).name, 'Forecast get rejected!', status.HTTP_400_BAD_REQUEST)
from rest_framework import status
from rest_framework.views import APIView
from app.enum import SnopStatus, SnopConfigurationKeys, EntitiesConfigurationKeys, ForecastConfigurationKeys, \
    SegmentationType, ForecastType, ForecastStatus, ResponseCodes, AdjustmentOperations
import logging
from app.utils import Util
from com_scai_dpai.utils import Util as base_util
from com_scai_dpai.decorators import user_access_permission
from django.utils.decorators import method_decorator
from app.services.forecast.adjustment.adjustment import ForecastAdjustmentService

logger = logging.getLogger(__name__)

class ForecastAdjustment(APIView):
    @method_decorator(user_access_permission(permissions=['FORECAST_UPDATE', 'COLLABORATION_UPDATE']))
    def put(self, request, forecast_type):
        try:
            forecastType = Util.castForecastType(forecast_type)
            if forecastType == 0:
                logger.error(f"DPAI Service: {forecast_type}ForecastAdjustment-PUT: Invalid ForecastType: tenant_id: {self.request.query_params.get('tenant_id')} | bu_id: {self.request.query_params.get('bu_id')} | snop_id: {self.request.query_params.get('snop_id')}")
                return base_util.createFailureResponse(self, ResponseCodes(5).name, 'Invalid Request', status.HTTP_404_NOT_FOUND)
            check_tenant_buid_snop_id = base_util.validateRequestParams(self, request)
            if check_tenant_buid_snop_id is False:
                logger.error(f"DPAI Service: {forecast_type}ForecastAdjustment-PUT: UNAUTHORIZED: tenant_id: {self.request.query_params.get('tenant_id')} | bu_id: {self.request.query_params.get('bu_id')} | snop_id: {self.request.query_params.get('snop_id')}")
                return base_util.createFailureResponse(self, ResponseCodes(2).name, 'Unauthorized Request', status.HTTP_401_UNAUTHORIZED)
            logger.debug(
                f"tenant_id: {request.GET.get('tenant_id')} | bu_id: {request.GET.get('bu_id')} | snop_id: {request.GET.get('snop_id')}")
            
            if request.data and ('period' in request.data and request.data['period']) and ('operation' in request.data and request.data['operation']) and ('adjustedVolume' in request.data and request.data['adjustedVolume']) and (request.data['operation'].upper() in [AdjustmentOperations(1).name, AdjustmentOperations(2).name, AdjustmentOperations(3).name, AdjustmentOperations(4).name]) and ('forecastIds' in request.data and request.data['forecastIds']):
                result = ForecastAdjustmentService.adjust(self, self.request.query_params.get('tenant_id'), self.request.query_params.get('bu_id'), self.request.query_params.get('snop_id'), request.data, str.replace(str(request.META.get('HTTP_AUTHORIZATION')), 'Bearer ', ''), ForecastType(forecastType).name)
            else:
                logger.error(f"DPAI Service: {forecast_type}ForecastAdjustment-PUT: Forecast adjustment updated rejected body: {request.data}")
                return base_util.createFailureResponse(self, ResponseCodes(5).name, 'Invalid Request Body!', status.HTTP_400_BAD_REQUEST)
            
            if result['status'] != 200:
                return base_util.createFailureResponse(self, result['responseCode'], result['responseMessage'], status.HTTP_400_BAD_REQUEST)
            else:
                return base_util.createSuccessResponse(self, result['data'], result['responseCode'], result['responseMessage'], status.HTTP_200_OK)
        except Exception as e:
            logger.error(f"DPAI Service: {forecast_type}ForecastAdjustment-PUT: Forecast adjustment updated rejected with error: {e}")
            return base_util.createFailureResponse(self, ResponseCodes(5).name, 'Forecast adjustment rejected!', status.HTTP_400_BAD_REQUEST)

    def get(self, request, forecast_type):
        try:
            forecastType = Util.castForecastType(forecast_type)
            if forecastType == 0:
                logger.error(
                    f"DPAI Service: {forecast_type}Forecast GetLogAdjustment: Invalid ForecastType: tenant_id: {self.request.query_params.get('tenant_id')} | bu_id: {self.request.query_params.get('bu_id')} | snop_id: {self.request.query_params.get('snop_id')}")
                return base_util.createFailureResponse(self, ResponseCodes(5).name, 'Invalid Request',
                                                      status.HTTP_404_NOT_FOUND)
            check_tenant_buid_snop_id = base_util.validateRequestParams(self, request)
            if check_tenant_buid_snop_id is False:
                logger.error(
                    f"DPAI Service: {forecast_type}Forecast GetLogAdjustment: UNAUTHORIZED: tenant_id: {self.request.query_params.get('tenant_id')} | bu_id: {self.request.query_params.get('bu_id')} | snop_id: {self.request.query_params.get('snop_id')}")
                return base_util.createFailureResponse(self, ResponseCodes(2).name, 'Unauthorized Request',
                                                       status.HTTP_401_UNAUTHORIZED)
            logger.debug(
                f"tenant_id: {request.GET.get('tenant_id')} | bu_id: {request.GET.get('bu_id')} | snop_id: {request.GET.get('snop_id')}")

            result = ForecastAdjustmentService.get(self, self.request.query_params.get('tenant_id'),
                                         self.request.query_params.get('bu_id'),
                                         self.request.query_params.get('snop_id'),
                                         str.replace(str(request.META.get('HTTP_AUTHORIZATION')), 'Bearer ', ''),
                                         ForecastType(forecastType).name)

            if result['status'] != 200:
                return base_util.createFailureResponse(self, result['responseCode'], result['responseMessage'], status.HTTP_400_BAD_REQUEST)
            else:
                return base_util.createSuccessResponse(self, result['data'], result['responseCode'], result['responseMessage'], status.HTTP_200_OK)
        except Exception as e:
            logger.error(
                f"DPAI Service: {forecast_type}Forecast GetLogAdjustment: Forecast get log_adjustment rejected with error: {e}")
            return base_util.createFailureResponse(self, ResponseCodes(5).name, 'Forecast get_log_adjustment rejected!',
                                                   status.HTTP_400_BAD_REQUEST)
        
    def post(self, request, forecast_type):
        try:
            forecastType = Util.castForecastType(forecast_type)
            if forecastType == 0:
                logger.error(f"DPAI Service: {forecast_type}Forecast GetLogAdjustment: Invalid ForecastType: tenant_id: {self.request.query_params.get('tenant_id')} | bu_id: {self.request.query_params.get('bu_id')} | snop_id: {self.request.query_params.get('snop_id')}")
                return base_util.createFailureResponse(self, ResponseCodes(5).name, 'Invalid Request',status.HTTP_404_NOT_FOUND)
            
            check_tenant_buid_snop_id = base_util.validateRequestParams(self, request)
            if check_tenant_buid_snop_id is False:
                logger.error(f"DPAI Service: {forecast_type}Forecast GetLogAdjustment: UNAUTHORIZED: tenant_id: {self.request.query_params.get('tenant_id')} | bu_id: {self.request.query_params.get('bu_id')} | snop_id: {self.request.query_params.get('snop_id')}")
                return base_util.createFailureResponse(self, ResponseCodes(2).name, 'Unauthorized Request',status.HTTP_401_UNAUTHORIZED)
            logger.debug(f"tenant_id: {request.GET.get('tenant_id')} | bu_id: {request.GET.get('bu_id')} | snop_id: {request.GET.get('snop_id')}")

            result = ForecastAdjustmentService.update(self, self.request.query_params.get('tenant_id'),self.request.query_params.get('bu_id'),self.request.query_params.get('snop_id'),str.replace(str(request.META.get('HTTP_AUTHORIZATION')), 'Bearer ', ''), ForecastType(forecastType).name, request.data)
            if result['status'] != 200:
                return base_util.createFailureResponse(self, result['responseCode'], result['responseMessage'], status.HTTP_400_BAD_REQUEST)
            else:
                return base_util.createSuccessResponse(self, result['data'], result['responseCode'], result['responseMessage'], status.HTTP_200_OK)
        except Exception as e:
            logger.error(f"DPAI Service: {forecast_type}Forecast GetLogAdjustment: Forecast get log_adjustment rejected with error: {e}")
            return base_util.createFailureResponse(self, ResponseCodes(5).name, 'Forecast get_log_adjustment rejected!',status.HTTP_400_BAD_REQUEST)

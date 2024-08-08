from rest_framework import status
from rest_framework.views import APIView
from app.enum import SnopStatus, SnopConfigurationKeys, EntitiesConfigurationKeys, ForecastConfigurationKeys, \
    SegmentationType, ForecastType, ForecastStatus, ResponseCodes
import logging
from app.utils import Util
from com_scai_dpai.utils import Util as base_util
from app.services.forecast.alert import AlertService
from com_scai_dpai.decorators import user_access_permission
from django.utils.decorators import method_decorator

logger = logging.getLogger(__name__)

class Alert(APIView):

    def get(self, request, forecast_type):
        try:
            forecastType = Util.castForecastType(forecast_type)
            if forecastType == 0:
                logger.error(f"DPAI Service: {forecast_type}Forecast-GET Alerts: Invalid ForecastType: tenant_id: {self.request.query_params.get('tenant_id')} | bu_id: {self.request.query_params.get('bu_id')} | snop_id: {self.request.query_params.get('snop_id')}")
                return base_util.createFailureResponse(self, ResponseCodes(5).name, 'Invalid Request', status.HTTP_404_NOT_FOUND)
            check_tenant_buid_snop_id = base_util.validateRequestParams(self, request)
            if check_tenant_buid_snop_id is False:
                logger.error(f"DPAI Service: {forecast_type}Forecast-GET Alerts: UNAUTHORIZED: tenant_id: {request.GET.get('tenant_id')} | bu_id: {request.GET.get('bu_id')} | snop_id: {request.GET.get('snop_id')}")
                return base_util.createFailureResponse(self, ResponseCodes(2).name, 'Unauthorized Request', status.HTTP_401_UNAUTHORIZED)
            logger.debug(
                f"tenant_id: {request.GET.get('tenant_id')} | bu_id: {request.GET.get('bu_id')} | snop_id: {request.GET.get('snop_id')}")
            
            result = AlertService.get(self, self.request.query_params.get('tenant_id'), self.request.query_params.get('bu_id'), self.request.query_params.get('snop_id'), str.replace(str(request.META.get('HTTP_AUTHORIZATION')), 'Bearer ', ''), ForecastType(forecastType).name)
            if result['status'] != 200:
                return base_util.createFailureResponse(self, result['responseCode'], result['responseMessage'], status.HTTP_400_BAD_REQUEST)
            else:
                return base_util.createSuccessResponse(self, result['data'], result['responseCode'], result['responseMessage'], status.HTTP_200_OK)
        except Exception as e:
            logger.error(f"DPAI Service: {forecast_type}Forecast-GET Alerts: Alert get rejected with error: {e}")
            return base_util.createFailureResponse(self, ResponseCodes(5).name, 'Alert get rejected!', status.HTTP_400_BAD_REQUEST)
        
    def post(self, request, forecast_type):
        try:
            forecastType = Util.castForecastType(forecast_type)
            if forecastType == 0:
                logger.error(f"DPAI Service: {forecast_type}Forecast-GET Alerts: Invalid ForecastType: tenant_id: {self.request.query_params.get('tenant_id')} | bu_id: {self.request.query_params.get('bu_id')} | snop_id: {self.request.query_params.get('snop_id')}")
                return base_util.createFailureResponse(self, ResponseCodes(5).name, 'Invalid Request', status.HTTP_404_NOT_FOUND)
            check_tenant_buid_snop_id = base_util.validateRequestParams(self, request)
            if check_tenant_buid_snop_id is False:
                logger.error(f"DPAI Service: {forecast_type}Forecast-GET Alerts: UNAUTHORIZED: tenant_id: {request.GET.get('tenant_id')} | bu_id: {request.GET.get('bu_id')} | snop_id: {request.GET.get('snop_id')}")
                return base_util.createFailureResponse(self, ResponseCodes(2).name, 'Unauthorized Request', status.HTTP_401_UNAUTHORIZED)
            logger.debug(
                f"tenant_id: {request.GET.get('tenant_id')} | bu_id: {request.GET.get('bu_id')} | snop_id: {request.GET.get('snop_id')}")
            
            result = AlertService.update(self, tenant_id = self.request.query_params.get('tenant_id'), bu_id = self.request.query_params.get('bu_id'), snop_id = self.request.query_params.get('snop_id'), token = str.replace(str(request.META.get('HTTP_AUTHORIZATION')), 'Bearer ', ''), forecast_type = ForecastType(forecastType).name, data = request.data)
            if result['status'] != 200:
                return base_util.createFailureResponse(self, result['responseCode'], result['responseMessage'], status.HTTP_400_BAD_REQUEST)
            else:
                return base_util.createSuccessResponse(self, result['data'], result['responseCode'], result['responseMessage'], status.HTTP_200_OK)

        except Exception as e:
            logger.error(f"DPAI Service: {forecast_type}Forecast-GET Alerts: Alert get rejected with error: {e}")
            return base_util.createFailureResponse(self, ResponseCodes(5).name, 'Alert get rejected!', status.HTTP_400_BAD_REQUEST)

    
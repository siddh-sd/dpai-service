from rest_framework import status
from app.model.forecast.forecast_header import ForecastHeader
from rest_framework.views import APIView
from datetime import datetime
from app.enum import SnopStatus, SnopConfigurationKeys, EntitiesConfigurationKeys, ForecastConfigurationKeys, \
    SegmentationType, ForecastType, ForecastStatus, ResponseCodes, AdjustmentOperations
import logging
from app.utils import Util
from com_scai_dpai.utils import Util as base_util
from com_scai_dpai.decorators import user_access_permission
from django.utils.decorators import method_decorator
from app.services.forecast.review.review import ForecastReviewService

logger = logging.getLogger(__name__)


class ForecastReject(APIView):

    @method_decorator(user_access_permission(permissions=['FORECAST_UPDATE', 'COLLABORATION_UPDATE']))
    def put(self, request, forecast_type):
        try:
            forecastType = Util.castForecastType(forecast_type)
            if forecastType == 0:
                logger.error(
                    f"DPAI Service: {forecast_type}ForecastReview-PUT: Invalid ForecastType: tenant_id: {self.request.query_params.get('tenant_id')} | bu_id: {self.request.query_params.get('bu_id')} | snop_id: {self.request.query_params.get('snop_id')}")
                return base_util.createFailureResponse(self, ResponseCodes(5).name, 'Invalid Request',
                                                       status.HTTP_404_NOT_FOUND)
            check_tenant_buid_snop_id = base_util.validateRequestParams(self, request)
            if check_tenant_buid_snop_id is False:
                logger.error(
                    f"DPAI Service: {forecast_type}ForecastReview-PUT: UNAUTHORIZED: tenant_id: {self.request.query_params.get('tenant_id')} | bu_id: {self.request.query_params.get('bu_id')} | snop_id: {self.request.query_params.get('snop_id')}")
                return base_util.createFailureResponse(self, ResponseCodes(2).name, 'Unauthorized Request',
                                                       status.HTTP_401_UNAUTHORIZED)
            logger.debug(
                f"tenant_id: {request.GET.get('tenant_id')} | bu_id: {request.GET.get('bu_id')} | snop_id: {request.GET.get('snop_id')}")

            requestHeaderIds = request.data['forecast_header_ids']

            if requestHeaderIds:
                result = ForecastReviewService.reject(self, self.request.query_params.get('tenant_id'),
                                                       self.request.query_params.get('bu_id'),
                                                       self.request.query_params.get('snop_id'), requestHeaderIds, request.data['remark_code'], request.data['remark_description'],
                                                       str.replace(str(request.META.get('HTTP_AUTHORIZATION')),
                                                                   'Bearer ', ''), ForecastType(forecastType).name)
            else:
                logger.error(
                    f"DPAI Service: {forecast_type}ForecastReview-PUT: Forecast review rejected body: {request.data}")
                return base_util.createFailureResponse(self, ResponseCodes(5).name, 'Invalid Request Body!',
                                                       status.HTTP_400_BAD_REQUEST)

            if result['status'] != 200:
                return base_util.createFailureResponse(self, result['responseCode'], result['responseMessage'],
                                                       status.HTTP_400_BAD_REQUEST)
            else:
                return base_util.createSuccessResponse(self, result['data'], result['responseCode'],
                                                       result['responseMessage'], status.HTTP_200_OK)

        except Exception as e:
            logger.error(
                f"DPAI Service: {forecast_type}ForecastReview-PUT: Forecast review rejected with error: {e}")
            return base_util.createFailureResponse(self, ResponseCodes(5).name, 'Forecast review rejected!',
                                                   status.HTTP_400_BAD_REQUEST)


from rest_framework import status
from rest_framework.views import APIView
from datetime import datetime
import logging
from snop.serializers import SnopSerializer
from snop.models import Snop
from com_scai_dpai.utils import Util as base_util
from django.utils.decorators import method_decorator
from com_scai_dpai.decorators import user_access_permission
from snop.enum import ResponseCodes

logger = logging.getLogger(__name__)

class DefaultSnop(APIView):
    @method_decorator(user_access_permission(permissions=['SNOP_VIEW']))
    def get(self, request):
        current_date = datetime.now().date()
        bu_id = request.GET.get('bu_id')

        check_tenant_buid_snop_id = base_util.validateRequestParams(self, request)
        if check_tenant_buid_snop_id is False:
            logger.error(f"UNAUTHORIZED: tenant_id: {request.GET.get('tenant_id')} | bu_id: {request.GET.get('bu_id')} | snop_id: {request.GET.get('snop_id')}")
            return base_util.createFailureResponse(self, ResponseCodes(20).name, 'Unauthorized Request', status.HTTP_401_UNAUTHORIZED)
        logger.info(
            f"tenant_id: {request.GET.get('tenant_id')} | bu_id: {request.GET.get('bu_id')} | snop_id: {request.GET.get('snop_id')}")
        snops = Snop.objects.filter(bu_id=bu_id, from_date__gt = current_date, snop_date__gte = current_date ,is_active=True).order_by('from_date')
        serializer = SnopSerializer(snops, many=True)
        return base_util.createSuccessResponse(self, serializer.data[0] if len(serializer.data) != 0 else {}, ResponseCodes(1).name, 'Successfull!', status.HTTP_200_OK)

from rest_framework import status
from rest_framework.views import APIView
from datetime import datetime
from snop.enum import SnopStatus, ResponseCodes
import logging
from snop.serializers import SnopSerializer
from snop.models import Snop as snop_model
from django.utils.decorators import method_decorator
from com_scai_dpai.decorators import user_access_permission
from django.conf import settings
from com_scai_dpai.utils import Util as base_util
from com_scai_dpai.helper.login import Login

logger = logging.getLogger(__name__)


class Snop(APIView):
    @method_decorator(user_access_permission(permissions=['SNOP_VIEW']))
    def get(self, request):
        try:
            bu_id = request.GET.get('bu_id')
            check_tenant_buid_snop_id = base_util.validateRequestParams(self, request)
            if check_tenant_buid_snop_id is False:
                logger.error(
                    f"UNAUTHORIZED: tenant_id: {request.GET.get('tenant_id')} | bu_id: {request.GET.get('bu_id')} | snop_id: {request.GET.get('snop_id')}")
                return base_util.createFailureResponse(self, ResponseCodes(20).name, 'Unauthorized Request',
                                                       status.HTTP_401_UNAUTHORIZED)
            logger.info(
                f"tenant_id: {request.GET.get('tenant_id')} | bu_id: {request.GET.get('bu_id')} | snop_id: {request.GET.get('snop_id')}")

            snops = snop_model.objects.filter(bu_id=bu_id, is_active=True)
            serializer = SnopSerializer(snops, many=True)
            return base_util.createSuccessResponse(self, serializer.data, ResponseCodes(1).name, 'Successfull!',
                                                   status.HTTP_200_OK)
        except Exception as e:
            logger.error(f"SNOP GET ERROR: {e}")
            return base_util.createFailureResponse(self, ResponseCodes(2).name, 'data not found',
                                                   status.HTTP_400_BAD_REQUEST)

    @method_decorator(user_access_permission(permissions=['SNOP_CREATE']))
    def post(self, request):
        try:
            userEmail = base_util.getLoggedInUserEmailAddress(self, str.replace(str(request.META.get('HTTP_AUTHORIZATION')), 'Bearer ', ''))
            params = dict(self.request.query_params.dict())
            bu_id = self.request.query_params.get('bu_id')
            tenant_id = params['tenant_id']
            check_tenant_buid = base_util.validateRequestParams(self, request)

            if check_tenant_buid is False:
                logger.error(f"UNAUTHORIZED: tenant_id: {tenant_id} | bu_id: {bu_id}")
                return base_util.createFailureResponse(self, ResponseCodes(20).name, 'Unauthorized Request',
                                                       status.HTTP_401_UNAUTHORIZED)


            new_request = request.data
            new_request["bu_id"] = bu_id
            new_request["created_by"] = base_util.getLoggedInUserId(str.replace(str(request.META.get('HTTP_AUTHORIZATION')), 'Bearer ', ''))
            new_request["updated_at"] = datetime.now()
            new_request["updated_by"] = base_util.getLoggedInUserId(str.replace(str(request.META.get('HTTP_AUTHORIZATION')), 'Bearer ', ''))
            new_request["created_at"] = datetime.now()
            token = Login.get_token(settings.SYS_ADMIN_EMAIL, settings.SYS_ADMIN_PASSWORD)
            serializer = SnopSerializer(data=new_request, context={'tenant_id': tenant_id,'request': request,'token': token})

            if serializer.is_valid():
                serializer.save()
                return base_util.createSuccessResponse(self, serializer.data, ResponseCodes(3).name,
                                                       'SNOP has been created successfully', status.HTTP_201_CREATED)

            error = list(serializer.errors.values())
            logger.error(f"SNOP CREATE ERROR: {error}")
            return base_util.createFailureResponse(self, error[0][0], error[1][0], status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            logger.error(f"SNOP CREATE ERROR: {e}")
            return base_util.createFailureResponse(self, ResponseCodes(4).name, 'SNOP Rejected',
                                                   status.HTTP_400_BAD_REQUEST)

    @method_decorator(user_access_permission(permissions=['SNOP_EDIT']))
    def put(self, request):
        try:
            userEmail = base_util.getLoggedInUserEmailAddress(self, str.replace(str(request.META.get('HTTP_AUTHORIZATION')), 'Bearer ', ''))
            params = dict(self.request.query_params.dict())
            bu_id = params['bu_id']
            tenant_id = params['tenant_id']
            check_tenant_buid_snop_id = base_util.validateRequestParams(self, request)
            if check_tenant_buid_snop_id is False:
                logger.error(f"UNAUTHORIZED: tenant_id: {tenant_id} | bu_id: {bu_id}")
                return base_util.createFailureResponse(self, ResponseCodes(20).name, 'Unauthorized Request',
                                                       status.HTTP_401_UNAUTHORIZED)

            snop_obj = snop_model.objects.get(pk=request.data['snop_id'])
            if snop_obj.is_active:
                if str(snop_obj.from_date) != request.data["from_date"] or str(snop_obj.to_date) != request.data[
                    "to_date"]:
                    logger.error(
                        f"SNOP UPDATE ERROR: From date {str(snop_obj.from_date)} and To date {str(snop_obj.to_date)} cannot be modified")
                    return base_util.createFailureResponse(self, ResponseCodes(21).name,
                                                           'From date and To date cannot be modified',
                                                           status.HTTP_400_BAD_REQUEST)

                request.data["updated_at"] = datetime.now()
                request.data["bu_id"] = bu_id
                request.data["updated_by"] = base_util.getLoggedInUserId(str.replace(str(request.META.get('HTTP_AUTHORIZATION')), 'Bearer ', ''))
                token = Login.get_token(settings.SYS_ADMIN_EMAIL, settings.SYS_ADMIN_PASSWORD)
                serializer = SnopSerializer(snop_obj, data=request.data, partial=True,
                                                context={'tenant_id': tenant_id,'request': request,'token': token})
                if serializer.is_valid():
                    serializer.save()
                    return base_util.createSuccessResponse(self, serializer.data, ResponseCodes(22).name,
                                                           'SNOP Updated Successfully', status.HTTP_200_OK)
                error = list(serializer.errors.values())
                logger.error(f"SNOP UPDATE ERROR: {error}")
                return base_util.createFailureResponse(self, error[0][0], error[1][0], status.HTTP_400_BAD_REQUEST)
            else:
                return base_util.createFailureResponse(self, ResponseCodes(23).name, 'SNOP Update Rejected',
                                                       status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            logger.error(f"SNOP UPDATE ERROR: {e}")
            return base_util.createFailureResponse(self, ResponseCodes(23).name, 'SNOP Update Rejected',
                                                   status.HTTP_400_BAD_REQUEST)

    @method_decorator(user_access_permission(permissions=['SNOP_DELETE']))
    def delete(self, request):
        try:
            bu_id = self.request.query_params.get('bu_id')
            tenant_id = self.request.query_params.get('tenant_id')
            check_tenant_buid_snop_id = base_util.validateRequestParams(self, request)
            if check_tenant_buid_snop_id is False:
                logger.error(f"UNAUTHORIZED: tenant_id: {tenant_id} | bu_id: {bu_id}")
                return base_util.createFailureResponse(self, ResponseCodes(20).name, 'Unauthorized Request',
                                                       status.HTTP_401_UNAUTHORIZED)
            snop_obj = snop_model.objects.get(
                pk=self.request.query_params.get('snop_id'))
            if snop_obj.is_active:
                snop_obj.is_active = False
                snop_obj.save()
                return base_util.createSuccessResponse(self, {}, ResponseCodes(24).name, 'SNOP Deleted Successfully',
                                                       status.HTTP_200_OK)
            else:
                return base_util.createFailureResponse(self, ResponseCodes(25).name, 'SNOP Delete Rejected',
                                                       status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            logger.error(f"SNOP DELETE ERROR: {e}")
            return base_util.createFailureResponse(self, ResponseCodes(25).name, 'SNOP Delete Rejected',
                                                   status.HTTP_400_BAD_REQUEST)

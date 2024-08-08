from rest_framework.response import Response
from rest_framework import status
from tenant.models import Client
from tenant.serializers import TenantSerializer
from rest_framework.views import APIView
import logging
from django.utils.decorators import method_decorator
from com_scai_dpai.decorators import user_access_permission
logger = logging.getLogger(__name__)

class TenantController(APIView):
    @method_decorator(user_access_permission(permissions=['']))
    def post(self, request):
        try:
            token = request.META['HTTP_AUTHORIZATION']
            if token is None:
                return Response({'Error': 'Unauthorized Request'}, status=status.HTTP_401_UNAUTHORIZED)
        except Exception as e:
            logger.info(f"ERROR: {e}")
            return Response({'Error': 'Unauthorized Request'}, status=status.HTTP_401_UNAUTHORIZED)
        serializer = TenantSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response({'Successes': 'Tenant Created', 'data': serializer.data}, status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    def get(self, request):
        tenant_id = request.GET.get('tenant_id')

        try:
            if tenant_id is not None:
                tenant_obj = Client.objects.get(id=tenant_id)
                serializer = TenantSerializer(tenant_obj)
                return Response({'msg': 'succeess', 'payload': serializer.data}, status.HTTP_200_OK)

        except Exception as e:
            print(e)
            return Response({'payload': {}, 'msg': 'No data found'}, status.HTTP_400_BAD_REQUEST)

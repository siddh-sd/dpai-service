from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist
from django.db import connection
from django.http import HttpResponse
from django_tenants.utils import get_tenant_model
from tenant_schemas.middleware import BaseTenantMiddleware
from tenant_schemas.utils import get_public_schema_name
import logging
from rest_framework.response import Response
from querystring_parser import parser

logger = logging.getLogger(__name__)


class RequestIDTenantMiddleware(BaseTenantMiddleware):
    TENANT_NOT_FOUND_EXCEPTION = 'ERROR'

    def get_tenant(self, model, hostname, request):

        try:
            public_schema = model.objects.get(schema_name=get_public_schema_name())
        except ObjectDoesNotExist:

            public_schema = model.objects.create(
                domain_url=hostname,
                schema_name=get_public_schema_name(),
                tenant_id=get_public_schema_name().capitalize())
        public_schema.save()

        try:
            if (request.META['PATH_INFO'] == '/dp/tenant' and request.META['HTTP_AUTHORIZATION']):
                return public_schema
        except Exception as e:
            return Response("Invalid Request")
        try:
            post_dict = parser.parse(request.GET.urlencode())
            x_request_id = post_dict["tenant_id"]
            logger.info(f"Middleware request tenant_id :: {x_request_id}")
        except Exception as e:
            print(e)
            x_request_id = None
        try:
            post_dict = parser.parse(request.GET.urlencode())
            bu_id = post_dict["bu_id"]
            logger.info(f"Middleware request bu_id :: {bu_id}")
        except Exception as e:
            return Response(f"ERROR bu_id not passed::{e}")

        if x_request_id and bu_id:
            try:
                tenant_model = model.objects.get(schema_name="scai_dpai_" + x_request_id.replace("-", "_"))
                return tenant_model
            except Exception as e:
                return Response(f"ERROR tenant does not exist::{e}")

        else:
            return public_schema
    def process_request(self, request):
        # Connection needs first to be at the public schema, as this is where
        # the tenant metadata is stored.
        connection.set_schema_to_public()

        hostname = self.hostname_from_request(request)
        TenantModel = get_tenant_model()

        try:
            # get_tenant must be implemented by extending this class.
            tenant = self.get_tenant(TenantModel, hostname, request)
            assert isinstance(tenant, TenantModel)
        except TenantModel.DoesNotExist:
            raise self.TENANT_NOT_FOUND_EXCEPTION(
                "No tenant for {!r}".format(request.get_host())
            )
        except AssertionError:
            return HttpResponse('bu_id or tenant_id not found', status=404)

        request.tenant = tenant
        connection.set_tenant(request.tenant)

        # Do we have a public-specific urlconf?
        if (
                hasattr(settings, "PUBLIC_SCHEMA_URLCONF")
                and request.tenant.schema_name == get_public_schema_name()
        ):
            request.urlconf = settings.PUBLIC_SCHEMA_URLCONF

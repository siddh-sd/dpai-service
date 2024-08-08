from django.core.exceptions import ObjectDoesNotExist
from tenant_schemas.middleware import BaseTenantMiddleware
from tenant_schemas.utils import get_public_schema_name
import logging
from querystring_parser import parser

logger = logging.getLogger(__name__)


class RequestIDTenantMiddleware(BaseTenantMiddleware):

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
            post_dict = parser.parse(request.GET.urlencode())
            x_request_id = post_dict["tenant_id"]
            logger.info(f"Middleware request tenant_id :: {x_request_id}")
        except Exception as e:
            print(e)
            x_request_id = None
        if x_request_id:
            try:
                tenant_model = model.objects.get(schema_name="scai_dpai_" + x_request_id.replace("-", "_"))
                return tenant_model
            except Exception as e:
                return public_schema
        else:
            return public_schema

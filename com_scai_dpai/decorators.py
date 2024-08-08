from rest_framework import exceptions
from django.core.exceptions import PermissionDenied
import jwt
from querystring_parser import parser
import logging
logger = logging.getLogger(__name__)

def get_data_from_token(request):
    try:
        decoded = jwt.decode(str.replace(str(request.META.get('HTTP_AUTHORIZATION')), 'Bearer ', ''),
                                           options={"verify_signature": False})
        return decoded
    except jwt.ExpiredSignatureError as ex:
        logger.error(f"Error get_data_from_token: {ex}")
        raise exceptions.AuthenticationFailed('Token has expired')


def user_access_permission(**keywords):
    def decorator(view_func):
        def wrap(request, *args, **kwargs):
            bu_id_status = False
            tenant_id_status = False
            decoded = get_data_from_token(request)
            permissions = decoded['iam/permissions']
            roles = decoded['iam/roles']
            permission_name = keywords['permissions'] if keywords['permissions'] else None           
            post_dict = parser.parse(request.GET.urlencode())
            if 'SYS_ADMIN' in roles:
                return view_func(request, *args, **kwargs)
            
            try:
                tenant_id = post_dict["tenant_id"]
            except Exception as ex:
                logger.error({"user_access_permission: tenant_id not passed": f"{ex}"})
                raise PermissionDenied

            try:
                bu_id = post_dict["bu_id"]
                if decoded['business_unit_id'] == bu_id:
                    bu_id_status = True
            except Exception as ex:
                logger.error({"user_access_permission: bu_id not passed": f"{ex}"})
                pass

            try:
                if decoded['tenant_id'] == tenant_id and (True if "*" in permission_name else any(perm in permission_name for perm in permissions)):
                    tenant_id_status = True
            except Exception as ex:
                logger.error({"user_access_permission: Permission Denied": f"{ex}"})
                raise PermissionDenied

            if not permission_name or not bu_id_status or not tenant_id_status:
                logger.error({"user_access_permission: Permission Denied": f"decoded: {decoded}, PERMISSION: {permission_name}, BUID: {bu_id_status}, TENANTID: {tenant_id_status}"})
                raise PermissionDenied

            if '*' in permission_name:
                return view_func(request, *args, **kwargs)
            elif any(perm in permission_name for perm in permissions):
                return view_func(request, *args, **kwargs)                      
            else:                    
                logger.error("user_access_permission: Permission Denied")
                raise PermissionDenied
                
        return wrap

    return decorator

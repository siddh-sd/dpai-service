import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from django.conf import settings
import logging

logger = logging.getLogger(__name__)
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

class Product:
    @staticmethod
    def get(tenant_id, bu_id, token):
        productUrl = settings.ENTITIES_API_URL + settings.PRODUCT_API_BASE_URL + f"?tenant_id={tenant_id}&bu_id={bu_id}&requiredColumns=id,sku_name,channel_id,channel_name,like_sku_id,unit_price,product_lifecycle_status,lifecycle_start_date,lifecycle_end_date,lifecycle_sunset_date,sku_code,like_sku_name"
        headers =  {"Authorization": f"Bearer {token}"}
        response = requests.get(productUrl, headers=headers)
        
        if response.status_code != 200:
            logger.error(f"DPAI Service: Product-Get: Product API response with status code: {response.status_code} | tenant_id: {tenant_id} | bu_id: {bu_id}")
            return []
        
        logger.debug(f"Product API response with status code  : {response.status_code}")
        response = response.json()
        
        if response['responseData'] and response['responseData']['entities']:
            return response['responseData']['entities']

        return []
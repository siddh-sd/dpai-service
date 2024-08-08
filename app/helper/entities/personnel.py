import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from django.conf import settings
import logging

logger = logging.getLogger(__name__)
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


class Personnel:
    @staticmethod
    def get(tenant_id, bu_id, token, additionalColumns=""):
        personnelUrl = settings.ENTITIES_API_URL + settings.PERSONNEL_API_BASE_URL + f"?tenant_id={tenant_id}&bu_id={bu_id}&requiredColumns=sku_id,node_id,channel_id{additionalColumns}"
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(personnelUrl, headers=headers)

        if response.status_code != 200:
            logger.error(
                f"DPAI Service: Personnel-Get: Personnel API response with status code: {response.status_code} | tenant_id: {tenant_id} | bu_id: {bu_id}")
            return []

        logger.debug(f"Personnel API response with status code  : {response.status_code}")
        response = response.json()

        if response['responseData'] and response['responseData']['entities']:
            return response['responseData']['entities']

        return []
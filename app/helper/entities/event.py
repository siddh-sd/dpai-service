import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from django.conf import settings
import logging

logger = logging.getLogger(__name__)
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

class Event:
    @staticmethod
    def get(tenant_id, bu_id, token):
        eventUrl = settings.ENTITIES_API_URL + settings.EVENT_API_BASE_URL + f"?tenant_id={tenant_id}&bu_id={bu_id}"
        headers =  {"Authorization": f"Bearer {token}"}
        response = requests.get(eventUrl, headers=headers)
        
        if response.status_code != 200:
            logger.error(f"DPAI Service: Event-Get: Event API response with status code: {response.status_code} | tenant_id: {tenant_id} | bu_id: {bu_id} | snop_id: {snop_id}")
            return []
        
        logger.debug(f"Event API response with status code  : {response.status_code}")
        response = response.json()
        
        if response['responseData'] and response['responseData']['entities']:
            return response['responseData']['entities']

        return []
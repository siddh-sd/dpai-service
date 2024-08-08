import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from django.conf import settings
import logging

logger = logging.getLogger(__name__)
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

class ForecastSummary:
    @staticmethod
    def post(tenant_id, bu_id, snop_id, body, token):
        forecastSummaryUrl = settings.ENTITIES_API_URL + settings.FORECAST_SUMMARY_API_BASE_URL + f"?tenant_id={tenant_id}&bu_id={bu_id}&snop_id={snop_id}"
        headers =  {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        response = requests.post(forecastSummaryUrl, data=body, headers=headers)
        
        if response.status_code != 200:
            logger.error(f"DPAI Service: ForecastSummary-POST: ForecastSummary API response with status code: {response.status_code} | tenant_id: {tenant_id} | bu_id: {bu_id}")
            return []
        
        logger.debug(f"ForecastSummary API response with status code  : {response.status_code}")
        response = response.json()
        
        if response['responseData'] and response['responseData']['responseCode']:
            return response['responseData']['responseCode']

        return []
from rest_framework.response import Response
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from django.conf import settings
import logging
logger = logging.getLogger(__name__)
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
import httpx

class Email:
    @staticmethod
    def post(tenant_id, body, token):
        try:
            alert_base_url = settings.ALERTS_API_URL + settings.ALERTS_API_BASE_URL
            query_params = f"?tenant_id={tenant_id}"
            alert_url = alert_base_url + query_params
            headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
            requests.post(alert_url, json=body, headers=headers, timeout=1)
        except Exception as e:
            logger.error(f"DPAI Service: Email POST: Exception: {e}")
            # return {"responseCode":ResponseCodes(5).name,"responseMessage": "Forecast save rejected!", "status": status.HTTP_400_BAD_REQUEST}

        # if response.status_code != 200:
        #     logger.error("DPAI Service : Email : post : %s", response.text)
from rest_framework.response import Response
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from django.conf import settings

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

class Configuration:
    @staticmethod
    def get(tenant_id, bu_id, token):
        config_base_url = settings.CONFIGURATION_API_URL + settings.CONFIGURATION_API_BASE_URL
        query_params = f"?tenant_id={tenant_id}&bu_id={bu_id}"
        config_url = config_base_url+query_params
        headers =  {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
        response = requests.get(config_url, headers=headers)
        if response.status_code != 200:
            return Response({'msg': 'FAILED'})
        response = response.json()
        return response["responseData"]["response"]
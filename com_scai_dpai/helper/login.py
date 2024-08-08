import requests
import json
from django.conf import settings

class Login:
    @staticmethod
    def get_token(email, password):
        url = settings.IAM_API_URL + settings.LOGIN_API_BASE_URL
        data = {
                    "email": email,
                    "password": password
                }
        p = requests.post(url, json=data)
        data = p.json()
        json_str = json.dumps(data)
        resp = json.loads(json_str)
        token = resp['data']['access']
        return token
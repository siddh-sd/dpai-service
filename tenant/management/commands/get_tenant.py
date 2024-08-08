from django.core.management.base import BaseCommand
from django_tenants.utils import tenant_context, get_tenant_model
from snop.models import Snop
from django.core import serializers

import logging
logger = logging.getLogger(__name__)
import requests
from django.conf import settings
import json


class Command(BaseCommand):
    help = 'Displays hello.'

    def handle(self, *args, **kwargs):
        try:
            result = []
            for tenant in get_tenant_model().objects.all():
                with tenant_context(tenant):
                    logger.info(f'ALL TENANT IDs : {str(tenant.tenant_id)}')
                    tenant_id = str(tenant.tenant_id)
                    print(tenant_id)
                    if tenant_id != "Public":
                        snop_obj = Snop.objects.filter(is_active=False)

                        s_json_list = []
                        for obj in snop_obj:
                            snop_json = serializers.serialize("json", [obj])
                            json_ob = json.loads(snop_json)
                            for i in range(len(json_ob)):
                                s_json_list.append(json_ob[i]['fields'])

                            logger.info(f'snop_obj : {str(obj.snop_name)}')
                            logger.info(f'snop_id : {obj}')
                            logger.info(f'tenant_id : {tenant_id}')
                            logger.info(f'bu_id : {obj.buid}')

                            '''token API integration'''

                            url = "https://scai-dev-iam.3sc.ai/api/v1/users/login/"
                            token_dict = {
                                "email": "ankit.goyal@3scsolution.com",
                                "password": "admin123"
                            }
                            t_res = requests.post(url, json=token_dict)
                            token_data = t_res.json()
                            access_token = token_data['data']['access']

                            headers_auth = {
                                "Authorization": f'Bearer {access_token}'
                            }

                            '''personnel API integration'''
                            personnel_api_url = settings.ENTITIES_BASE_URL + f"/personnel/v1?tenant_id={tenant_id}&bu_id={obj.buid}"
                            logger.info("Requesting Personnel data from : " + f'{personnel_api_url}')
                            personnel_resp = requests.get(personnel_api_url, verify=False, headers=headers_auth)
                            logger.info(" personnel Response : " + f'{personnel_resp}')

                            if personnel_resp.status_code != 200:
                                personnel_response = "Error"
                            else:
                                personnel_response = personnel_resp.json()['responseData']['entities']


                            '''mapping API integration'''
                            mapping_api_url = settings.ENTITIES_BASE_URL + f"/mapping/v1?routeType={'mapping'}&tenant_id={tenant_id}&bu_id={obj.buid}"
                            logger.info("Requesting mapping data from : " + f'{mapping_api_url}')
                            mapping_resp = requests.get(mapping_api_url, verify=False, headers=headers_auth)
                            logger.info(" mapping Response : " + f'{mapping_resp}')

                            if mapping_resp.status_code != 200:
                                mapping_response = "Error"
                            else:
                                mapping_response = mapping_resp.json()['responseData']['entities']

                            '''location API integration'''
                            location_api_url = settings.ENTITIES_BASE_URL + f"/location/v1?tenant_id={tenant_id}&bu_id={obj.buid}"
                            logger.info("Requesting location data from : " + f'{location_api_url}')
                            location_resp = requests.get(location_api_url, verify=False, headers=headers_auth)
                            logger.info(" location Response : " + f'{location_resp}')

                            if location_resp.status_code != 200:
                                location_response = "Error"
                            else:
                                location_response = location_resp.json()["responseData"]["entities"]

                            '''Network API integration'''
                            network_api_url = settings.ENTITIES_BASE_URL + f"/network/v1?routeType={'network'}&tenant_id={tenant_id}&bu_id={obj.buid}&pageIndex=1&pageSize=3"
                            logger.info("Requesting network data from : " + f'{network_api_url}')
                            network_resp = requests.get(network_api_url, verify=False, headers=headers_auth)
                            logger.info(" Network Response : " + f'{network_resp}')

                            if network_resp.status_code != 200:
                                network_response = "Error"
                            else:
                                network_response = network_resp.json()["responseData"]["entities"]

                            '''Product API integration'''
                            product_api_url = settings.ENTITIES_BASE_URL + f"/product/v1?tenant_id={tenant_id}&bu_id={obj.buid}"
                            logger.info("Requesting Product data from : " + f'{product_api_url}')
                            product_resp = requests.get(product_api_url, verify=False, headers=headers_auth)
                            logger.info(" Product Response : " + f'{product_resp}')

                            if product_resp.status_code != 200:
                                product_response = "Error"
                            else:
                                product_response = product_resp.json()['responseData']['entities']

                            '''configuration API integration'''

                            configuration_api_url = settings.CONFIGURATION_BASE_URL + f"/configuration/v1?tenant_id={tenant_id}&buid={obj.buid}"
                            logger.info("Requesting configuration data from : " + f'{configuration_api_url}')
                            config_resp = requests.get(configuration_api_url, verify=False, headers=headers_auth)
                            logger.info(" configuration Response : " + f'{config_resp}')
                            logger.info(" configuration Response : " + f'{config_resp.status_code}')

                            if config_resp.status_code != 200:
                                configuration_response = "Error"
                            else:
                                configuration_response = config_resp.json()

                            '''historicalSales API integration'''
                            historicalSales_api_url = settings.CONFIGURATION_BASE_URL + f"/transactional/saleshistory/v1?bu_id={obj.buid}&tenant_id={tenant_id}"

                            logger.info("Requesting historicalSales data from : " + f'{historicalSales_api_url}')
                            historical_resp = requests.get(historicalSales_api_url, verify=False, headers=headers_auth)
                            logger.info(" historicalSales Response : " + f'{historical_resp}')

                            if historical_resp.status_code != 200:
                                historicalSales_response = "Error"
                            else:
                                historicalSales_response = historical_resp.json()['responseData']['entities']

                            '''TransactionSales feature API integration'''
                            transactionSales_api_url = settings.CONFIGURATION_BASE_URL + f"/dp/feature/v1?pageSize=3&pageIndex=1&tenant_id={tenant_id}&bu_id=100"
                            logger.info("Requesting historicalSales data from : " + f'{transactionSales_api_url}')
                            transactionSales_resp = requests.get(transactionSales_api_url, verify=False, headers=headers_auth)
                            logger.info(" TransactionSales Response : " + f'{transactionSales_resp}')

                            if transactionSales_resp.status_code != 200:
                                transactionSales_response = "Error"
                            else:
                                transactionSales_response = transactionSales_resp.json()['responseData']['entities']

                            api_responce = {
                                "configuration": configuration_response,
                                "data": {
                                    "snop": s_json_list,
                                    "product": product_response,
                                    "location": location_response,
                                    "personnel": personnel_response,
                                    "network": network_response,
                                    "mapping": mapping_response,
                                    "historicalSales": historicalSales_response,
                                    "features": transactionSales_response,
                                }
                            }

                            result.append(api_responce)
                with open('test_data.json', 'w', encoding='utf-8') as f:
                    json.dump(result, f, ensure_ascii=False, indent=4)

            self.stdout.write("Done")
        except Exception as e:
            logger.info("Something wrong : " + f'{e}')

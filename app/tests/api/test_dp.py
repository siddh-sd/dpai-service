import json
import pytest
from rest_framework.reverse import reverse
from unittest import mock
import requests


@pytest.mark.django_db
@mock.patch('app.utils.configurations.snop.get_confiurations')
@mock.patch('app.utils.utils.Util.get_tenant_buid')
@mock.patch('app.utils.utils.Util.decoded_token')
def test_snop_create_valid_daily(mock_get_confiurations, mock_get_tenant_buid, mock_decoded_token, client):
    mock_get_tenant_buid.return_value = True
    mock_decoded_token.return_value = "test@gmail.com"
    mock_get_confiurations.return_value = {
    "httpStatus": "OK",
    "responseData": {
        "response": {
            "entities": {
                "locationHierarchy": {
                    "locationHierarchyNoOfLevels": "5",
                    "locationHierarchyLevel1": "Area",
                    "locationHierarchyLevel2": "City",
                    "locationHierarchyLevel3": "State",
                    "locationHierarchyLevel4": "Region"
                },
                "productHierarchy": {
                    "productHierarchyNoOfLevels": 4,
                    "productHierarchyLevel1": "Product",
                    "productHierarchyLevel2": "Sub-Category",
                    "productHierarchyLevel3": "Category",
                    "productHierarchyLevel4": "Brand"
                },
                "salesmanHierarchy": {
                    "salesmanHierarchyNoOfLevels": 4,
                    "salesmanHierarchyLevel1": "ASM",
                    "salesmanHierarchyLevel2": "RSM",
                    "salesmanHierarchyLevel3": "ZSM",
                    "salesmanHierarchyLevel4": "NSM"
                },
                "demandPlannerHierarchy": {
                    "demandPlannerHierarchyNoOfLevels": 2,
                    "demandPlannerHierarchyLevel1": "Demand Planner",
                    "demandPlannerHierarchyLevel2": "Demand Planner Manager"
                }
            },
            "snop": {
                "planningCycleFrequency": "WEEKLY",
                "planningHorizon": 5,
                "actualSales": "daily",
                "granularity": "daily",
                "weekStart": "Tuesday"
            },
            "forecast": {
                "isDCActive": True,
                "ABCClassificationType": "DYNAMIC",
                "ABCClassificationHistoricalHorizon": 50,
                "productForecastLevel": 3,
                "locationForecastLevel": 2,
                "forecastGenerationDay": "1",
                "confidenceUpperLowerBound": "5",
                "errorSelectionCriteria": "MAPE",
                "definitionClassificationA": "85%",
                "definitionClassificationB": "10%",
                "definitionClassificationC": "5%",
                "NPIDays": "365",
                "sparsityHigh": "50%",
                "sparsityMedium": "30%",
                "sparsityLow": "20%",
                "XYZHigh": "50%",
                "XYZMedium": "30%",
                "XYZLow": "20%",
                "smoothDemand": "ADI < 1.32 and CV² < 0.49",
                "intermittentDemand": "ADI >= 1.32 and CV² < 0.49",
                "erraticDemand": "ADI < 1.32 and CV² >= 0.49",
                "lumpyDemand": "ADI >= 1.32 and CV² >= 0.49",
                "IQRMethod": "",
                "zScore": "",
                "seasonal": "",
                "decomposition": "",
                "cluster": ""
            },
            "locale": {
                "language": "en",
                "labelsDateFormat": "DD MMM YYYY",
                "datepickerFormat": "dd MMM yyyy",
                "datepickerFormatForQuaterly": "day month year",
                "dateFormat": "DD MMM YYYY",
                "decimalSeparator": ".",
                "thousandsGroupStyle": "million",
                "thousandSeparator": ","
            },
            "transactional": {
                "salesAndFeatureProductAttribute": "3",
                "salesAndFeatureLocationAttribute": "2",
                "featurePeriod": "Daily/Monthly/Weekly/Yearly",
                "numberOfFeatures": "2251",
                "feature1": "Temprature",
                "feature2": "Precipitation",
                "visibleSalesHistory": 5
            }
        },
        "responseMessage": "Successfully updated",
        "responseCode": "200"
    },
    "message": "Successfully got all records"
}

    payload = {
        "snop_name": "kk",
        "from_date": "2023-01-31",
        "to_date": "2023-02-02",
        "demand_review_date": "2023-01-05",
        "supply_review_date": "2023-01-06",
        "pre_snop_date": "2023-01-07",
        "snop_date": "2023-01-08",
        "forecast_trigger_date": "2023-01-04"
    }


    '''token API integration'''
    url = "https://scai-dev-iam.3sc.ai/api/v1/users/login/"
    token_dict = {
        # "email": "ankit.goyal@3scsolution.com",
        "email": "DP1@client1.com",
        "password": "admin123"
    }
    t_res = requests.post(url, json=token_dict)
    token_data = t_res.json()
    access_token = token_data['data']['access']

    # headers_auth = {
    #     "Authorization": f'Bearer {access_token}'
    # }

    header = {'HTTP_AUTHORIZATION': access_token}
    url = reverse('snop')
    response = client.post(url + f"?tenant_id=e7373db9-80bf-4f8d-864f-6f871105f6c3&bu_id=0ba06f15-a525-4588-93f2-40c0df757438", data=json.dumps(payload), content_type="application/json", **header)
    print("------->", response)
    #assert data["Successes"] == 'SNOP Created'
    assert response.status_code == 201

#
# @pytest.mark.django_db
# @mock.patch('app.snop_config.snop.get_confiurations')
# def test_snop_create_invalid_daily(mock_get_confiurations, client):
#     mock_get_confiurations.return_value = dict(snopconfiguration={
#         "planningCycleFrequency": "daily",
#         "planningHorizon": 3,
#         "Actualsales": "daily",
#         "Granularity": "daily",
#         "weekStart": "Thursday"
#     })
#
#     payload = {
#         "snop_name": "jay222",
#         "from_date": "2022-10-12",
#         "to_date": "2022-10-19",
#         "demand_review_date": "2022-09-02",
#         "supply_review_date": "2022-09-03",
#         "pre_snop_date": "2022-09-04",
#         "snop_date": "2022-09-05",
#         "buid": "1",
#         "tenant_id": "1234567"
#     }
#     url = reverse('snop')
#     response = client.post(url, data=json.dumps(payload), content_type="application/json")
#     assert response.status_code == 400
#
#
# @pytest.mark.django_db
# @mock.patch('app.snop_config.snop.get_confiurations')
# def test_snop_create_valid_weekly(mock_get_confiurations, client):
#     mock_get_confiurations.return_value = dict(snopconfiguration={
#         "planningCycleFrequency": "weekly",
#         "planningHorizon": 3,
#         "Actualsales": "daily",
#         "Granularity": "daily",
#         "weekStart": "Tuesday"
#     })
#
#     payload = {
#         "snop_name": "jay222",
#         "from_date": "2022-10-04",
#         "to_date": "2022-10-24",
#         "demand_review_date": "2022-09-02",
#         "supply_review_date": "2022-09-03",
#         "pre_snop_date": "2022-09-04",
#         "snop_date": "2022-09-05",
#         "buid": "1",
#         "tenant_id": "1"
#     }
#     url = reverse('snop')
#     response = client.post(url, data=json.dumps(payload), content_type="application/json")
#     data = response.data
#     assert data["Successes"] == 'SNOP Created'
#     assert response.status_code == 201
#
#
# @pytest.mark.django_db
# @mock.patch('app.snop_config.snop.get_confiurations')
# def test_snop_create_invalid_weekly(mock_get_confiurations, client):
#     mock_get_confiurations.return_value = dict(snopconfiguration={
#         "planningCycleFrequency": "weekly",
#         "planningHorizon": 3,
#         "Actualsales": "daily",
#         "Granularity": "daily",
#         "weekStart": "sunday"
#     })
#
#     payload = {
#         "snop_name": "jay222",
#         "from_date": "2022-10-04",
#         "to_date": "2022-10-24",
#         "demand_review_date": "2022-09-02",
#         "supply_review_date": "2022-09-03",
#         "pre_snop_date": "2022-09-04",
#         "snop_date": "2022-09-05",
#         "buid": "1",
#         "tenant_id": "1"
#     }
#     url = reverse('snop')
#     response = client.post(url, data=json.dumps(payload), content_type="application/json")
#     assert response.status_code == 400
#
#
# @pytest.mark.django_db
# @mock.patch('app.snop_config.snop.get_confiurations')
# def test_snop_create_valid_monthly(mock_get_confiurations, client):
#     mock_get_confiurations.return_value = dict(snopconfiguration={
#         "planningCycleFrequency": "monthly",
#         "planningHorizon": 3,
#         "Actualsales": "daily",
#         "Granularity": "daily",
#         "weekStart": "friday"
#     })
#
#     payload = {
#         "snop_name": "jay222",
#         "from_date": "2022-10-01",
#         "to_date": "2022-12-31",
#         "demand_review_date": "2022-09-02",
#         "supply_review_date": "2022-09-03",
#         "pre_snop_date": "2022-09-04",
#         "snop_date": "2022-09-05",
#         "buid": "1",
#         "tenant_id": "1"
#     }
#     url = reverse('snop')
#     response = client.post(url, data=json.dumps(payload), content_type="application/json")
#     data = response.data
#     assert data["Successes"] == 'SNOP Created'
#     assert response.status_code == 201
#
#
# @pytest.mark.django_db
# @mock.patch('app.snop_config.snop.get_confiurations')
# def test_snop_create_invalid_monthly(mock_get_confiurations, client):
#     mock_get_confiurations.return_value = dict(snopconfiguration={
#         "planningCycleFrequency": "monthly",
#         "planningHorizon": 3,
#         "Actualsales": "daily",
#         "Granularity": "daily",
#         "weekStart": "friday"
#     })
#
#     payload = {
#         "snop_name": "jay222",
#         "from_date": "2022-10-01",
#         "to_date": "2022-11-15",
#         "demand_review_date": "2022-09-02",
#         "supply_review_date": "2022-09-03",
#         "pre_snop_date": "2022-09-04",
#         "snop_date": "2022-09-05",
#         "buid": "1",
#         "tenant_id": "1"
#     }
#     url = reverse('snop')
#     response = client.post(url, data=json.dumps(payload), content_type="application/json")
#     assert response.status_code == 400
#
#
# @pytest.mark.django_db
# @mock.patch('app.snop_config.snop.get_confiurations')
# def test_snop_create_valid_fortnights(mock_get_confiurations, client):
#     mock_get_confiurations.return_value = dict(snopconfiguration={
#         "planningCycleFrequency": "fortnightly",
#         "planningHorizon": 3,
#         "Actualsales": "daily",
#         "Granularity": "daily",
#         "weekStart": "friday"
#     })
#
#     payload = {
#         "snop_name": "jay222",
#         "from_date": "2022-10-01",
#         "to_date": "2022-11-14",
#         "demand_review_date": "2022-09-02",
#         "supply_review_date": "2022-09-03",
#         "pre_snop_date": "2022-09-04",
#         "snop_date": "2022-09-05",
#         "buid": "1",
#         "tenant_id": "1"
#     }
#     url = reverse('snop')
#     response = client.post(url, data=json.dumps(payload), content_type="application/json")
#     data = response.data
#     assert data["Successes"] == 'SNOP Created'
#     assert response.status_code == 201
#
#
# @pytest.mark.django_db
# @mock.patch('app.snop_config.snop.get_confiurations')
# def test_snop_create_invalid_fortnights(mock_get_confiurations, client):
#     mock_get_confiurations.return_value = dict(snopconfiguration={
#         "planningCycleFrequency": "fortnightly",
#         "planningHorizon": 3,
#         "Actualsales": "daily",
#         "Granularity": "daily",
#         "weekStart": "friday"
#     })
#
#     payload = {
#         "snop_name": "jay222",
#         "from_date": "2022-10-01",
#         "to_date": "2022-10-10",
#         "demand_review_date": "2022-09-02",
#         "supply_review_date": "2022-09-03",
#         "pre_snop_date": "2022-09-04",
#         "snop_date": "2022-09-05",
#         "buid": "1",
#         "tenant_id": "1"
#     }
#     url = reverse('snop')
#     response = client.post(url, data=json.dumps(payload), content_type="application/json")
#     assert response.status_code == 400
#
#
# @pytest.mark.django_db
# @mock.patch('app.snop_config.snop.get_confiurations')
# def test_snop_create_valid_quarterly(mock_get_confiurations, client):
#     mock_get_confiurations.return_value = dict(snopconfiguration={
#         "planningCycleFrequency": "quarterly",
#         "planningHorizon": 3,
#         "Actualsales": "daily",
#         "Granularity": "daily",
#         "weekStart": "friday"
#     })
#
#     payload = {
#         "snop_name": "jay222",
#         "from_date": "2022-10-01",
#         "to_date": "2023-06-30",
#         "demand_review_date": "2022-09-02",
#         "supply_review_date": "2022-09-03",
#         "pre_snop_date": "2022-09-04",
#         "snop_date": "2022-09-05",
#         "buid": "1",
#         "tenant_id": "1"
#     }
#     url = reverse('snop')
#     response = client.post(url, data=json.dumps(payload), content_type="application/json")
#     data = response.data
#     assert data["Successes"] == 'SNOP Created'
#     assert response.status_code == 201
#
#
# @pytest.mark.django_db
# @mock.patch('app.snop_config.snop.get_confiurations')
# def test_snop_create_invalid_quarterly(mock_get_confiurations, client):
#     mock_get_confiurations.return_value = dict(snopconfiguration={
#         "planningCycleFrequency": "quarterly",
#         "planningHorizon": 3,
#         "Actualsales": "daily",
#         "Granularity": "daily",
#         "weekStart": "friday"
#     })
#
#     payload = {
#         "snop_name": "jay222",
#         "from_date": "2022-10-12",
#         "to_date": "2023-04-30",
#         "demand_review_date": "2022-09-02",
#         "supply_review_date": "2022-09-03",
#         "pre_snop_date": "2022-09-04",
#         "snop_date": "2022-09-05",
#         "buid": "1",
#         "tenant_id": "1"
#     }
#     url = reverse('snop')
#     response = client.post(url, data=json.dumps(payload), content_type="application/json")
#     assert response.status_code == 400
#
#
# @pytest.mark.django_db
# @mock.patch('app.snop_config.snop.get_confiurations')
# def test_snop_create_invalid_demand_supply_presnop_snop(mock_get_confiurations, client):
#     mock_get_confiurations.return_value = dict(snopconfiguration={
#         "planningCycleFrequency": "quarterly",
#         "planningHorizon": 3,
#         "Actualsales": "daily",
#         "Granularity": "daily",
#         "weekStart": "friday"
#     })
#
#     payload = {
#         "snop_name": "jay222",
#         "from_date": "2022-10-01",
#         "to_date": "2023-06-30",
#         "demand_review_date": "2022-09-08",
#         "supply_review_date": "2022-09-03",
#         "pre_snop_date": "2022-09-14",
#         "snop_date": "2022-09-05",
#         "buid": "1",
#         "tenant_id": "1"
#     }
#     url = reverse('snop')
#     response = client.post(url, data=json.dumps(payload), content_type="application/json")
#     assert response.status_code == 400
#
#
# @pytest.mark.django_db
# @mock.patch('app.snop_config.snop.get_confiurations')
# def test_snop_create_valid_demand_supply_presnop_snop(mock_get_confiurations, client):
#     mock_get_confiurations.return_value = dict(snopconfiguration={
#         "planningCycleFrequency": "quarterly",
#         "planningHorizon": 3,
#         "Actualsales": "daily",
#         "Granularity": "daily",
#         "weekStart": "friday"
#     })
#
#     payload = {
#         "snop_name": "jay222",
#         "from_date": "2022-10-01",
#         "to_date": "2023-06-30",
#         "demand_review_date": "2022-09-02",
#         "supply_review_date": "2022-09-03",
#         "pre_snop_date": "2022-09-04",
#         "snop_date": "2022-09-05",
#         "buid": "1",
#         "tenant_id": "1"
#     }
#     url = reverse('snop')
#     response = client.post(url, data=json.dumps(payload), content_type="application/json")
#     data = response.data
#     assert data["Successes"] == 'SNOP Created'
#     assert response.status_code == 201
#
#
# @pytest.mark.django_db
# @mock.patch('app.snop_config.snop.get_confiurations')
# def test_snop_edit_valid(mock_get_confiurations, snop, client):
#     print(snop)
#     mock_get_confiurations.return_value = dict(snopconfiguration={
#         "planningCycleFrequency": "quarterly",
#         "planningHorizon": 3,
#         "Actualsales": "daily",
#         "Granularity": "daily",
#         "weekStart": "friday"
#     })
#
#     payload = {
#         "snop_id": 1,
#         "snop_name": "jay22222",
#         "from_date": "2022-10-01",
#         "to_date": "2023-06-30",
#         "demand_review_date": "2022-09-02",
#         "supply_review_date": "2022-09-03",
#         "pre_snop_date": "2022-09-04",
#         "snop_date": "2022-09-06",
#         "buid": "1",
#         "tenant_id": "1"
#     }
#     url = reverse('snop')
#     response = client.put(url, data=json.dumps(payload), content_type="application/json")
#     data = response.data
#     assert data["Successes"] == 'SNOP Updated'
#     assert response.status_code == 200
#
#
# @pytest.mark.django_db
# @mock.patch('app.snop_config.snop.get_confiurations')
# def test_snop_edit_invalid(mock_get_confiurations, snop, client):
#     mock_get_confiurations.return_value = dict(snopconfiguration={
#         "planningCycleFrequency": "quarterly",
#         "planningHorizon": 3,
#         "Actualsales": "daily",
#         "Granularity": "daily",
#         "weekStart": "friday"
#     })
#
#     payload = {
#         "snop_id": 1,
#         "snop_name": "jay22222",
#         "from_date": "2022-10-05",
#         "to_date": "2023-06-30",
#         "demand_review_date": "2022-09-02",
#         "supply_review_date": "2022-09-03",
#         "pre_snop_date": "2022-09-04",
#         "snop_date": "2022-09-06",
#         "buid": "1",
#         "tenant_id": "1"
#     }
#     url = reverse('snop')
#     response = client.put(url, data=json.dumps(payload), content_type="application/json")
#     assert response.status_code == 400
#
#
# @pytest.mark.django_db
# @mock.patch('app.snop_config.snop.get_confiurations')
# def test_snop_get_valid(mock_get_confiurations, snop, client):
#     mock_get_confiurations.return_value = dict(snopconfiguration={
#         "planningCycleFrequency": "quarterly",
#         "planningHorizon": 3,
#         "Actualsales": "daily",
#         "Granularity": "daily",
#         "weekStart": "friday"
#     })
#     url = reverse('snop')
#     response = client.delete(url + f"?snop_id={1}", content_type="application/json")
#     data = response.data
#     assert data["Success"] == 'Record Deleted'
#     assert response.status_code == 204

import json
import pytest
from rest_framework.reverse import reverse
from unittest import mock
from rest_framework import status

access_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3MiOiJhY2Nlc3MiLCJleHAiOjE2NzQwMTgzODAsImlhdCI6MTY3MzkzMTk4MCwianRpIjoiN2I1YzJkZTcwNjY4NDMyOWJlMjdlNzA4MTM2MmZjNjUiLCJ1c2VyX2lkIjoiMjdjMWIwZWQtM2ZiZC00YTJkLTgyZmQtNmEwYzMxMGQ4YTAxIiwicm9sZXMiOlsiREVNQU5EMTIiXSwicGVybWlzc2lvbnMiOlsiU05PUF9FRElUIiwiU0FMRVNISVNUT1JZX0NSRUFURUVESVQiLCJTTk9QX0RFTEVURSIsIk5FVFdPUktfQ1JFQVRFRURJVCIsIlNBTEVTSElTVE9SWV9WSUVXIiwiRkVBVFVSRVNfVklFVyIsIkxPQ0FUSU9OX1ZJRVciLCJORVRXT1JLX1ZJRVciLCJQRVJTT05ORUxfQ1JFQVRFRURJVCIsIk1BUFBJTkdfQ1JFQVRFRURJVCIsIlNOT1BfQ1JFQVRFIiwiTUFQUElOR19WSUVXIiwiUEVSU09OTkVMX1ZJRVciLCJTTk9QX1ZJRVciLCJTQUxFU0ZPUkVDQVNUX0FESlVTVE1FTlQiLCJTQUxFU0ZPUkVDQVNUX1ZJRVciLCJTQUxFU0ZPUkVDQVNUX1VQREFURSIsIkZFQVRVUkVTX0NSRUFURUVESVQiLCJGT1JFQ0FTVF9WSUVXIiwiQU5BTFlUSUNTX1ZJRVciLCJQUk9EVUNUX1ZJRVciLCJQUk9EVUNUX0NSRUFURUVESVQiLCJVTkNPTlNUUkFJTkVEX1ZJRVciLCJPUEVSQVRJT05BTF9WSUVXIiwiTE9DQVRJT05fQ1JFQVRFRURJVCJdLCJ0ZW5hbnRfaWQiOiJlNzM3M2RiOS04MGJmLTRmOGQtODY0Zi02Zjg3MTEwNWY2YzMiLCJ0ZW5hbnRfbmFtZSI6ImNsaWVudDEiLCJlbWFpbCI6ImRwMUBjbGllbnQxLmNvbSIsImJ1c2luZXNzX3VuaXRfaWQiOiIwYmEwNmYxNS1hNTI1LTQ1ODgtOTNmMi00MGMwZGY3NTc0MzgiLCJidXNpbmVzc191bml0X25hbWUiOiJjbGllbnQxLWJ1MSJ9.jYdJhpPpr7NRPsIw024E8pbiwg6_YWIE__fKKdgYU4A"

@pytest.mark.django_db
@mock.patch('app.snop_config.snop.get_confiurations')
def test_forecast_operational_edit_valid(mock_get_confiurations, snop, forecast_header, forecast_approval,
                                         forecast_detail, client):
    snop_id = snop.snop_id
    bu_id = snop.bu_id

    mock_get_confiurations.return_value = dict(
        snopconfiguration=
        {
            "planningCycleFrequency": "daily",
            "planningHorizon": 3,
            "Actualsales": "daily",
            "Granularity": "daily",
            "weekStart": "Tuesday",
            "isDCActive": True
        },
        forecast=
        {
            "ABCClassificationType": "STATIC",
            "productForecastLevel": "3",
            "locationForecastLevel": "2"
        },
        locationHierarchy=
        {
            "locationHierarchyNoOfLevels": 4,
            "locationHierarchyLevel1": "Area",
            "locationHierarchyLevel2": "City",
            "locationHierarchyLevel3": "State",
            "locationHierarchyLevel4": "Region",
        },
        productHierarchy=
        {
            "productHierarchyNoOfLevels": 4,
            "productHierarchyLevel1": "Product",
            "productHierarchyLevel2": "Sub-Category",
            "productHierarchyLevel3": "Category",
            "productHierarchyLevel4": "Brand",
        },
        transactional=
        {
            "salesAndFeatureProductAttribute": "3",
            "salesAndFeatureLocationAttribute": "2",
            "featurePeriod": "Daily/Monthly/Weekly/Yearly",
            "numberOfFeatures": "4",
            "feature1": "Temprature",
            "feature2": "Precipitation",
            "feature3": "Feature3",
        },
        demandPlannerHierarchy=
        {
            "demandPlannerHierarchyNoOfLevels": 3,
            "demandPlannerHierarchyLevel1": "Demand Planner1",
            "demandPlannerHierarchyLevel2": "Demand Planner2",
            "demandPlannerHierarchyLevel3": "Demand Planner3"
        },
        salesmanHierarchy=
        {
            "salesmanHierarchyNoOfLevels": 3,
            "salesmanHierarchyLevel1": "ASM",
            "salesmanHierarchyLevel2": "RSM",
            "salesmanHierarchyLevel3": "ZSM"
        }
    )
    payload = [
        {
            "forecast_header_id_fk": 1,
            "Operational":
                [
                    {
                        "forecast_detail_id": 5,
                        "volume": 300
                    },
                    {
                        "forecast_detail_id": 6,
                        "volume": 201
                    },
                    {
                        "forecast_detail_id": 7,
                        "volume": 203
                    }
                ]
        }
    ]

    header = {'HTTP_AUTHORIZATION': access_token}
    url = reverse('forecast_operational')

    response = client.put(url + f"?tenant_id=e7373db9-80bf-4f8d-864f-6f871105f6c3&bu_id={bu_id}&snop_id={snop_id}",
                          data=json.dumps(payload), content_type="application/json", **header)
    print('response', response)
    data = response.data
    print("data", data)

    assert data['Success'] == 'Forecast operational updated'
    assert response.status_code == status.HTTP_200_OK


@pytest.mark.django_db
@mock.patch('app.snop_config.snop.get_confiurations')
def test_forecast_operational_get_valid(mock_get_confiurations, snop, forecast_header, forecast_approval,
                                        forecast_detail, client):
    snop_id = snop.snop_id
    bu_id = snop.bu_id

    mock_get_confiurations.return_value = dict(
        snopconfiguration=
        {
            "planningCycleFrequency": "daily",
            "planningHorizon": 3,
            "Actualsales": "daily",
            "Granularity": "daily",
            "weekStart": "Tuesday",
            "isDCActive": True
        },
        forecast=
        {
            "ABCClassificationType": "STATIC",
            "productForecastLevel": "3",
            "locationForecastLevel": "2"
        },
        locationHierarchy=
        {
            "locationHierarchyNoOfLevels": 4,
            "locationHierarchyLevel1": "Area",
            "locationHierarchyLevel2": "City",
            "locationHierarchyLevel3": "State",
            "locationHierarchyLevel4": "Region",
        },
        productHierarchy=
        {
            "productHierarchyNoOfLevels": 4,
            "productHierarchyLevel1": "Product",
            "productHierarchyLevel2": "Sub-Category",
            "productHierarchyLevel3": "Category",
            "productHierarchyLevel4": "Brand",
        },
        transactional=
        {
            "salesAndFeatureProductAttribute": "3",
            "salesAndFeatureLocationAttribute": "2",
            "featurePeriod": "Daily/Monthly/Weekly/Yearly",
            "numberOfFeatures": "4",
            "feature1": "Temprature",
            "feature2": "Precipitation",
            "feature3": "Feature3",
        },
        demandPlannerHierarchy=
        {
            "demandPlannerHierarchyNoOfLevels": 3,
            "demandPlannerHierarchyLevel1": "Demand Planner1",
            "demandPlannerHierarchyLevel2": "Demand Planner2",
            "demandPlannerHierarchyLevel3": "Demand Planner3"
        },
        salesmanHierarchy=
        {
            "salesmanHierarchyNoOfLevels": 3,
            "salesmanHierarchyLevel1": "ASM",
            "salesmanHierarchyLevel2": "RSM",
            "salesmanHierarchyLevel3": "ZSM"
        }
    )

    header = {'HTTP_AUTHORIZATION': access_token}
    url = reverse('forecast_operational')

    response = client.get(url + f"?tenant_id=e7373db9-80bf-4f8d-864f-6f871105f6c3&bu_id={bu_id}&snop_id={snop_id}",
                          data={}, content_type="application/json", **header)
    print('---------response----------', response)
    data = response.data
    print("-----------data-------------", data)

    assert data['msg'] == 'SUCCESS'
    assert response.status_code == status.HTTP_200_OK


@pytest.mark.django_db
@mock.patch('app.snop_config.snop.get_confiurations')
def test_forecast_operational_review_edit_valid(mock_get_confiurations, snop, forecast_header, forecast_approval,
                                                forecast_detail, client):
    snop_id = snop.snop_id
    bu_id = snop.bu_id

    mock_get_confiurations.return_value = dict(
        snopconfiguration=
        {
            "planningCycleFrequency": "daily",
            "planningHorizon": 3,
            "Actualsales": "daily",
            "Granularity": "daily",
            "weekStart": "Tuesday",
            "isDCActive": True
        },
        forecast=
        {
            "ABCClassificationType": "STATIC",
            "productForecastLevel": "3",
            "locationForecastLevel": "2"
        },
        locationHierarchy=
        {
            "locationHierarchyNoOfLevels": 4,
            "locationHierarchyLevel1": "Area",
            "locationHierarchyLevel2": "City",
            "locationHierarchyLevel3": "State",
            "locationHierarchyLevel4": "Region",
        },
        productHierarchy=
        {
            "productHierarchyNoOfLevels": 4,
            "productHierarchyLevel1": "Product",
            "productHierarchyLevel2": "Sub-Category",
            "productHierarchyLevel3": "Category",
            "productHierarchyLevel4": "Brand",
        },
        transactional=
        {
            "salesAndFeatureProductAttribute": "3",
            "salesAndFeatureLocationAttribute": "2",
            "featurePeriod": "Daily/Monthly/Weekly/Yearly",
            "numberOfFeatures": "4",
            "feature1": "Temprature",
            "feature2": "Precipitation",
            "feature3": "Feature3",
        },
        demandPlannerHierarchy=
        {
            "demandPlannerHierarchyNoOfLevels": 3,
            "demandPlannerHierarchyLevel1": "Demand Planner1",
            "demandPlannerHierarchyLevel2": "Demand Planner2",
            "demandPlannerHierarchyLevel3": "Demand Planner3"
        },
        salesmanHierarchy=
        {
            "salesmanHierarchyNoOfLevels": 3,
            "salesmanHierarchyLevel1": "ASM",
            "salesmanHierarchyLevel2": "RSM",
            "salesmanHierarchyLevel3": "ZSM"
        }
    )
    payload = {
        "forecastHeaders": [1]
    }

    header = {'HTTP_AUTHORIZATION': access_token}
    url = reverse('forecast_operational_review')

    response = client.put(url + f"?tenant_id=e7373db9-80bf-4f8d-864f-6f871105f6c3&bu_id={bu_id}&snop_id={snop_id}",
                          data=json.dumps(payload), content_type="application/json", **header)
    print('response', response)
    data = response.data
    print("data", data)

    assert data['Success'] == 'Forecast operational review updated'
    assert response.status_code == status.HTTP_200_OK



# @pytest.mark.django_db
# @mock.patch('app.snop_config.snop.get_confiurations')
# def test_forecast_operational_edit_invalid_for_wrong_data(mock_get_confiurations, snop, forecast_header, forecast_detail, forecast_approval, forecast_personnel, forecast_demandplannerheirarchy, forecast_SalesmanHeirarchy, client):

#     mock_get_confiurations.return_value = dict(
#         snopconfiguration={
#             "planningCycleFrequency": "quarterly",
#             "planningHorizon": 4,
#             "Actualsales": "daily",
#             "Granularity": "daily",
#             "weekStart": "friday",
#             "IsDCActive": True
#         },
#         DemandPlannerHierarchy={
#             "DemandPlannerHierarchyNoOfLevels": 4,
#             "DemandPlannerHierarchyLevel1": "Demand Planner1",
#             "DemandPlannerHierarchyLevel2": "Demand Planner2",
#             "DemandPlannerHierarchyLevel3": "Demand Planner3",
#             "DemandPlannerHierarchyLevel4": "Demand Planner4"
#         },
#         SalesmanHierarchy={
#             "SalesmanHierarchyNoOfLevels": 4,
#             "SalesmanHierarchyLevel1": "ASM",
#             "SalesmanHierarchyLevel2": "RSM",
#             "SalesmanHierarchyLevel3": "ZSM",
#             "SalesmanHierarchyLevel4": "NSM"
#         },
#     )
#     payload = [
#         {
#             "forecast_header_id_fk": 1,
#             "sales":
#                 [
#                     {
#                         "forecast_detail_id": 1,
#                         "volume": "88"
#                     },
#                     {
#                         "forecast_detail_id": 2,
#                         "volume": 223
#                     },
#                     {
#                         "forecast_detail_id": 3,
#                         "volume": 224
#                     },
#                     {
#                         "forecast_detail_id": 7,
#                         "volume": 225
#                     }
#                 ]
#         },
#         {
#             "forecast_header_id_fk": 2,
#             "Operational":
#                 [
#                     {
#                         "forecast_detail_id": 4,
#                         "volume": 226
#                     },
#                     {
#                         "forecast_detail_id": 5,
#                         "volume": 227
#                     },
#                     {
#                         "forecast_detail_id": 6,
#                         "volume": 228
#                     },
#                     {
#                         "forecast_detail_id": 8,
#                         "volume": 123123
#                     }
#                 ]
#         }
#     ]

#     url = reverse('forecast_review_operational')
#     header = {'HTTP_EMAIL': 'test1@gmail.com', 'HTTP_SNOP_ID': '1'}
#     param = {'HTTP_TENANT_ID': '1'}
#     response = client.put(url, data=json.dumps(payload), content_type="application/json", **header, **param)
#     print('response', response)
#     data = response.data
#     print("data", data)

#    assert response.status_code == status.HTTP_400_BAD_REQUEST
#
#
# @pytest.mark.django_db
# @mock.patch('app.snop_config.snop.get_confiurations')
# def test_forecast_operational_edit_invalid_volume(mock_get_confiurations, snop, forecast_header, forecast_detail, forecast_approval, forecast_personnel, forecast_demandplannerheirarchy, forecast_SalesmanHeirarchy, client):
#
#     mock_get_confiurations.return_value = dict(
#         snopconfiguration={
#             "planningCycleFrequency": "quarterly",
#             "planningHorizon": 4,
#             "Actualsales": "daily",
#             "Granularity": "daily",
#             "weekStart": "friday",
#             "IsDCActive": True
#         },
#         DemandPlannerHierarchy={
#             "DemandPlannerHierarchyNoOfLevels": 4,
#             "DemandPlannerHierarchyLevel1": "Demand Planner1",
#             "DemandPlannerHierarchyLevel2": "Demand Planner2",
#             "DemandPlannerHierarchyLevel3": "Demand Planner3",
#             "DemandPlannerHierarchyLevel4": "Demand Planner4"
#         },
#         SalesmanHierarchy={
#             "SalesmanHierarchyNoOfLevels": 4,
#             "SalesmanHierarchyLevel1": "ASM",
#             "SalesmanHierarchyLevel2": "RSM",
#             "SalesmanHierarchyLevel3": "ZSM",
#             "SalesmanHierarchyLevel4": "NSM"
#         },
#     )
#
#     payload = [
#         {
#             "forecast_header_id_fk": 1,
#             "Operational":
#                 [
#                     {
#                         "forecast_detail_id": 1,
#                         "volume": 88
#                     },
#                     {
#                         "forecast_detail_id": 2,
#                         "volume": -223
#                     },
#                     {
#                         "forecast_detail_id": 3,
#                         "volume": 224
#                     },
#                     {
#                         "forecast_detail_id": 7,
#                         "volume": 225
#                     }
#                 ]
#         },
#         {
#             "forecast_header_id_fk": 2,
#             "Operational":
#                 [
#                     {
#                         "forecast_detail_id": 4,
#                         "volume": 226
#                     },
#                     {
#                         "forecast_detail_id": 5,
#                         "volume": 227
#                     },
#                     {
#                         "forecast_detail_id": 6,
#                         "volume": 228
#                     },
#                     {
#                         "forecast_detail_id": 8,
#                         "volume": 123123
#                     }
#                 ]
#         }
#     ]
#
#     url = reverse('forecast_review_operational')
#     header = {'HTTP_EMAIL': 'test1@gmail.com', 'HTTP_SNOP_ID': '1'}
#     param = {'HTTP_TENANT_ID': '1'}
#     response = client.put(url, data=json.dumps(payload), content_type="application/json", **header, **param)
#     print('response', response)
#     data = response.data
#     print("data", data)
#
#     assert data['Error'] == 'Volume needs to match planning horizon & it should be positive'
#     assert response.status_code == status.HTTP_400_BAD_REQUEST
#
#
# @pytest.mark.django_db
# @mock.patch('app.snop_config.snop.get_confiurations')
# def test_forecast_operational_edit_invalid_planningHorizon(mock_get_confiurations, snop, forecast_header, forecast_detail, forecast_approval, forecast_personnel, forecast_demandplannerheirarchy, forecast_SalesmanHeirarchy, client):
#
#     mock_get_confiurations.return_value = dict(
#         snopconfiguration={
#             "planningCycleFrequency": "quarterly",
#             "planningHorizon": 3,
#             "Actualsales": "daily",
#             "Granularity": "daily",
#             "weekStart": "friday",
#             "IsDCActive": True
#         },
#         DemandPlannerHierarchy={
#             "DemandPlannerHierarchyNoOfLevels": 4,
#             "DemandPlannerHierarchyLevel1": "Demand Planner1",
#             "DemandPlannerHierarchyLevel2": "Demand Planner2",
#             "DemandPlannerHierarchyLevel3": "Demand Planner3",
#             "DemandPlannerHierarchyLevel4": "Demand Planner4"
#         },
#         SalesmanHierarchy={
#             "SalesmanHierarchyNoOfLevels": 4,
#             "SalesmanHierarchyLevel1": "ASM",
#             "SalesmanHierarchyLevel2": "RSM",
#             "SalesmanHierarchyLevel3": "ZSM",
#             "SalesmanHierarchyLevel4": "NSM"
#         },
#     )
#
#     payload = [
#         {
#             "forecast_header_id_fk": 1,
#             "Operational":
#                 [
#                     {
#                         "forecast_detail_id": 1,
#                         "volume": 88
#                     },
#                     {
#                         "forecast_detail_id": 2,
#                         "volume": 223
#                     },
#                     {
#                         "forecast_detail_id": 3,
#                         "volume": 224
#                     },
#                     {
#                         "forecast_detail_id": 7,
#                         "volume": 225
#                     }
#                 ]
#         },
#         {
#             "forecast_header_id_fk": 2,
#             "Operational":
#                 [
#                     {
#                         "forecast_detail_id": 4,
#                         "volume": 226
#                     },
#                     {
#                         "forecast_detail_id": 5,
#                         "volume": 227
#                     },
#                     {
#                         "forecast_detail_id": 6,
#                         "volume": 228
#                     },
#                     {
#                         "forecast_detail_id": 8,
#                         "volume": 123123
#                     }
#
#                 ]
#         }
#     ]
#
#     url = reverse('forecast_review_operational')
#     header = {'HTTP_EMAIL': 'test1@gmail.com', 'HTTP_SNOP_ID': '1'}

#     response = client.put(url, data=json.dumps(payload), content_type="application/json", **header)
#     print('response', response)
#     data = response.data
#     print("data", data)
#
#     assert data['Error'] == 'Volume needs to match planning horizon & it should be positive'
#     assert response.status_code == status.HTTP_400_BAD_REQUEST
#
#
# @pytest.mark.django_db
# @mock.patch('app.snop_config.snop.get_confiurations')
# def test_forecast_operational_edit_invalid_user_email(mock_get_confiurations, snop, forecast_header, forecast_detail, forecast_approval, forecast_personnel, forecast_demandplannerheirarchy, forecast_SalesmanHeirarchy, client):
#
#     mock_get_confiurations.return_value = dict(
#         snopconfiguration={
#             "planningCycleFrequency": "quarterly",
#             "planningHorizon": 4,
#             "Actualsales": "daily",
#             "Granularity": "daily",
#             "weekStart": "friday",
#             "IsDCActive": True
#         },
#         DemandPlannerHierarchy={
#             "DemandPlannerHierarchyNoOfLevels": 4,
#             "DemandPlannerHierarchyLevel1": "Demand Planner1",
#             "DemandPlannerHierarchyLevel2": "Demand Planner2",
#             "DemandPlannerHierarchyLevel3": "Demand Planner3",
#             "DemandPlannerHierarchyLevel4": "Demand Planner4"
#         },
#         SalesmanHierarchy={
#             "SalesmanHierarchyNoOfLevels": 4,
#             "SalesmanHierarchyLevel1": "ASM",
#             "SalesmanHierarchyLevel2": "RSM",
#             "SalesmanHierarchyLevel3": "ZSM",
#             "SalesmanHierarchyLevel4": "NSM"
#         },
#     )
#
#     payload = [
#         {
#             "forecast_header_id_fk": 1,
#             "Operational":
#                 [
#                     {
#                         "forecast_detail_id": 1,
#                         "volume": 88
#                     },
#                     {
#                         "forecast_detail_id": 2,
#                         "volume": 223
#                     },
#                     {
#                         "forecast_detail_id": 3,
#                         "volume": 224
#                     },
#                     {
#                         "forecast_detail_id": 7,
#                         "volume": 225
#                     }
#                 ]
#         },
#         {
#             "forecast_header_id_fk": 2,
#             "Operational":
#                 [
#                     {
#                         "forecast_detail_id": 4,
#                         "volume": 226
#                     },
#                     {
#                         "forecast_detail_id": 5,
#                         "volume": 227
#                     },
#                     {
#                         "forecast_detail_id": 6,
#                         "volume": 228
#                     },
#                     {
#                         "forecast_detail_id": 8,
#                         "volume": 123123
#                     }
#                 ]
#         }
#     ]
#
#     url = reverse('forecast_review_operational')
#     header = {'HTTP_EMAIL': 'test@gmail.com', 'HTTP_SNOP_ID': '1'}
#     param = {'HTTP_TENANT_ID': '1'}
#     response = client.put(url, data=json.dumps(payload), content_type="application/json", **header, **param)
#     print('response', response)
#     data = response.data
#     print("data", data)
#
#     assert data['Error'] == 'Invalid user identified in email verification'
#     assert response.status_code == status.HTTP_400_BAD_REQUEST
#
#
# @pytest.mark.django_db
# @mock.patch('app.snop_config.snop.get_confiurations')
# def test_forecast_operational_edit_invalid_approved_till_level(mock_get_confiurations, snop, forecast_header, forecast_detail, forecast_approval_invalid_operational, forecast_personnel, forecast_demandplannerheirarchy, forecast_SalesmanHeirarchy, client):
#
#     mock_get_confiurations.return_value = dict(
#         snopconfiguration={
#             "planningCycleFrequency": "quarterly",
#             "planningHorizon": 4,
#             "Actualsales": "daily",
#             "Granularity": "daily",
#             "weekStart": "friday",
#             "IsDCActive": True
#         },
#         DemandPlannerHierarchy={
#             "DemandPlannerHierarchyNoOfLevels": 4,
#             "DemandPlannerHierarchyLevel1": "Demand Planner1",
#             "DemandPlannerHierarchyLevel2": "Demand Planner2",
#             "DemandPlannerHierarchyLevel3": "Demand Planner3",
#             "DemandPlannerHierarchyLevel4": "Demand Planner4"
#         },
#         SalesmanHierarchy={
#             "SalesmanHierarchyNoOfLevels": 4,
#             "SalesmanHierarchyLevel1": "ASM",
#             "SalesmanHierarchyLevel2": "RSM",
#             "SalesmanHierarchyLevel3": "ZSM",
#             "SalesmanHierarchyLevel4": "NSM"
#         },
#     )
#
#     payload = [
#         {
#             "forecast_header_id_fk": 1,
#             "Operational":
#                 [
#                     {
#                         "forecast_detail_id": 1,
#                         "volume": 88
#                     },
#                     {
#                         "forecast_detail_id": 2,
#                         "volume": 223
#                     },
#                     {
#                         "forecast_detail_id": 3,
#                         "volume": 224
#                     },
#                     {
#                         "forecast_detail_id": 7,
#                         "volume": 225
#                     }
#                 ]
#         },
#         {
#             "forecast_header_id_fk": 2,
#             "Operational":
#                 [
#                     {
#                         "forecast_detail_id": 4,
#                         "volume": 226
#                     },
#                     {
#                         "forecast_detail_id": 5,
#                         "volume": 227
#                     },
#                     {
#                         "forecast_detail_id": 6,
#                         "volume": 228
#                     },
#                     {
#                         "forecast_detail_id": 8,
#                         "volume": 123123
#                     }
#                 ]
#         }
#     ]
#
#     url = reverse('forecast_review_operational')
#     header = {'HTTP_EMAIL': 'test1@gmail.com', 'HTTP_SNOP_ID': '1'}
#     param = {'HTTP_TENANT_ID': '1'}
#     response = client.put(url, data=json.dumps(payload), content_type="application/json", **header, **param)
#     print('response', response)
#     data = response.data
#     print("data", data)
#
#     assert data['Error'] == 'Invalid user level'
#     assert response.status_code == status.HTTP_400_BAD_REQUEST
#
#
# @pytest.mark.django_db
# @mock.patch('app.snop_config.snop.get_confiurations')
# def test_forecast_operational_edit_invalid_IsDCActive(mock_get_confiurations, snop, forecast_header, forecast_detail, forecast_approval, forecast_personnel, forecast_demandplannerheirarchy, forecast_SalesmanHeirarchy, client):
#
#     mock_get_confiurations.return_value = dict(
#         snopconfiguration={
#             "planningCycleFrequency": "quarterly",
#             "planningHorizon": 4,
#             "Actualsales": "daily",
#             "Granularity": "daily",
#             "weekStart": "friday",
#             "IsDCActive": False
#         },
#         DemandPlannerHierarchy={
#             "DemandPlannerHierarchyNoOfLevels": 4,
#             "DemandPlannerHierarchyLevel1": "Demand Planner1",
#             "DemandPlannerHierarchyLevel2": "Demand Planner2",
#             "DemandPlannerHierarchyLevel3": "Demand Planner3",
#             "DemandPlannerHierarchyLevel4": "Demand Planner4"
#         },
#         SalesmanHierarchy={
#             "SalesmanHierarchyNoOfLevels": 4,
#             "SalesmanHierarchyLevel1": "ASM",
#             "SalesmanHierarchyLevel2": "RSM",
#             "SalesmanHierarchyLevel3": "ZSM",
#             "SalesmanHierarchyLevel4": "NSM"
#         },
#     )
#
#     payload = [
#         {
#             "forecast_header_id_fk": 1,
#             "Operational":
#                 [
#                     {
#                         "forecast_detail_id": 1,
#                         "volume": 88
#                     },
#                     {
#                         "forecast_detail_id": 2,
#                         "volume": 223
#                     },
#                     {
#                         "forecast_detail_id": 3,
#                         "volume": 224
#                     },
#                     {
#                         "forecast_detail_id": 7,
#                         "volume": 225
#                     }
#                 ]
#         },
#         {
#             "forecast_header_id_fk": 2,
#             "Operational":
#                 [
#                     {
#                         "forecast_detail_id": 4,
#                         "volume": 226
#                     },
#                     {
#                         "forecast_detail_id": 5,
#                         "volume": 227
#                     },
#                     {
#                         "forecast_detail_id": 6,
#                         "volume": 228
#                     },
#                     {
#                         "forecast_detail_id": 8,
#                         "volume": 123123
#                     }
#                 ]
#         }
#     ]
#
#     url = reverse('forecast_review_operational')
#     header = {'HTTP_EMAIL': 'test1@gmail.com', 'HTTP_SNOP_ID': '1'}
#     param = {'HTTP_TENANT_ID': '1'}
#     response = client.put(url, data=json.dumps(payload), content_type="application/json", **header, **param)
#     print('response', response)
#     data = response.data
#     print("data", data)
#
#     assert data['Error'] == 'config_data IsDCActive false'
#     assert response.status_code == status.HTTP_400_BAD_REQUEST
#
#
# @pytest.mark.django_db
# @mock.patch('app.snop_config.snop.get_confiurations')
# def test_forecast_operational_edit_invalid_snop_status(mock_get_confiurations, snop_invalid, forecast_header, forecast_detail, forecast_approval, forecast_personnel, forecast_demandplannerheirarchy, forecast_SalesmanHeirarchy, client):
#
#     mock_get_confiurations.return_value = dict(
#         snopconfiguration={
#             "planningCycleFrequency": "quarterly",
#             "planningHorizon": 4,
#             "Actualsales": "daily",
#             "Granularity": "daily",
#             "weekStart": "friday",
#             "IsDCActive": True
#         },
#         DemandPlannerHierarchy={
#             "DemandPlannerHierarchyNoOfLevels": 4,
#             "DemandPlannerHierarchyLevel1": "Demand Planner1",
#             "DemandPlannerHierarchyLevel2": "Demand Planner2",
#             "DemandPlannerHierarchyLevel3": "Demand Planner3",
#             "DemandPlannerHierarchyLevel4": "Demand Planner4"
#         },
#         SalesmanHierarchy={
#             "SalesmanHierarchyNoOfLevels": 4,
#             "SalesmanHierarchyLevel1": "ASM",
#             "SalesmanHierarchyLevel2": "RSM",
#             "SalesmanHierarchyLevel3": "ZSM",
#             "SalesmanHierarchyLevel4": "NSM"
#         },
#     )
#
#     payload = [
#         {
#             "forecast_header_id_fk": 1,
#             "Operational":
#                 [
#                     {
#                         "forecast_detail_id": 1,
#                         "volume": 88
#                     },
#                     {
#                         "forecast_detail_id": 2,
#                         "volume": 223
#                     },
#                     {
#                         "forecast_detail_id": 3,
#                         "volume": 224
#                     },
#                     {
#                         "forecast_detail_id": 7,
#                         "volume": 225
#                     }
#                 ]
#         },
#         {
#             "forecast_header_id_fk": 2,
#             "Operational":
#                 [
#                     {
#                         "forecast_detail_id": 4,
#                         "volume": 226
#                     },
#                     {
#                         "forecast_detail_id": 5,
#                         "volume": 227
#                     },
#                     {
#                         "forecast_detail_id": 6,
#                         "volume": 228
#                     },
#                     {
#                         "forecast_detail_id": 8,
#                         "volume": 123123
#                     }
#                 ]
#         }
#     ]
#
#     url = reverse('forecast_review_operational')
#     header = {'HTTP_EMAIL': 'test1@gmail.com', 'HTTP_SNOP_ID': '1'}
#     param = {'HTTP_TENANT_ID': '1'}
#     response = client.put(url, data=json.dumps(payload), content_type="application/json", **header, **param)
#     print('response', response)
#     data = response.data
#     print("data", data)
#
#     assert data['Error'] == 'SNOP status Not in progress'
#     assert response.status_code == status.HTTP_400_BAD_REQUEST
#
#
# @pytest.mark.django_db
# @mock.patch('app.snop_config.snop.get_confiurations')
# def test_forecast_operational_edit_invalid_snop_id(mock_get_confiurations, snop, forecast_header, forecast_detail, forecast_approval, forecast_personnel, forecast_demandplannerheirarchy, forecast_SalesmanHeirarchy, client):
#
#     mock_get_confiurations.return_value = dict(
#         snopconfiguration={
#             "planningCycleFrequency": "quarterly",
#             "planningHorizon": 4,
#             "Actualsales": "daily",
#             "Granularity": "daily",
#             "weekStart": "friday",
#             "IsDCActive": True
#         },
#         DemandPlannerHierarchy={
#             "DemandPlannerHierarchyNoOfLevels": 4,
#             "DemandPlannerHierarchyLevel1": "Demand Planner1",
#             "DemandPlannerHierarchyLevel2": "Demand Planner2",
#             "DemandPlannerHierarchyLevel3": "Demand Planner3",
#             "DemandPlannerHierarchyLevel4": "Demand Planner4"
#         },
#         SalesmanHierarchy={
#             "SalesmanHierarchyNoOfLevels": 4,
#             "SalesmanHierarchyLevel1": "ASM",
#             "SalesmanHierarchyLevel2": "RSM",
#             "SalesmanHierarchyLevel3": "ZSM",
#             "SalesmanHierarchyLevel4": "NSM"
#         },
#     )
#
#     payload = [
#         {
#             "forecast_header_id_fk": 1,
#             "Operational":
#                 [
#                     {
#                         "forecast_detail_id": 1,
#                         "volume": 88
#                     },
#                     {
#                         "forecast_detail_id": 2,
#                         "volume": 223
#                     },
#                     {
#                         "forecast_detail_id": 3,
#                         "volume": 224
#                     },
#                     {
#                         "forecast_detail_id": 7,
#                         "volume": 225
#                     }
#                 ]
#         },
#         {
#             "forecast_header_id_fk": 2,
#             "Operational":
#                 [
#                     {
#                         "forecast_detail_id": 4,
#                         "volume": 226
#                     },
#                     {
#                         "forecast_detail_id": 5,
#                         "volume": 227
#                     },
#                     {
#                         "forecast_detail_id": 6,
#                         "volume": 228
#                     },
#                     {
#                         "forecast_detail_id": 8,
#                         "volume": 123123
#                     }
#                 ]
#         }
#     ]
#
#     url = reverse('forecast_review_operational')
#     header = {'HTTP_EMAIL': 'test1@gmail.com', 'HTTP_SNOP_ID': '2'}
#     param = {'HTTP_TENANT_ID': '1'}
#     response = client.put(url, data=json.dumps(payload), content_type="application/json", **header, **param)
#     print('response', response)
#     data = response.data
#     print("data", data)
#
#     assert data['Error'] == 'SNOP id validation failed'
#     assert response.status_code == status.HTTP_400_BAD_REQUEST

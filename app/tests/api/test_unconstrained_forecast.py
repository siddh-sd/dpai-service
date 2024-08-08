import json
import pytest
from rest_framework.reverse import reverse
from unittest import mock
from rest_framework import status


@pytest.mark.django_db
@mock.patch('app.snop_config.snop.get_confiurations')
def test_forecast_unconstrained_edit_valid(mock_get_confiurations, snop, forecast_header,
                                           forecast_detail, forecast_approval, forecast_personnel,
                                           forecast_demandplannerheirarchy, forecast_SalesmanHeirarchy,
                                           client):
    mock_get_confiurations.return_value = dict(
        snopconfiguration={
            "planningCycleFrequency": "quarterly",
            "planningHorizon": 3,
            "Actualsales": "daily",
            "Granularity": "daily",
            "weekStart": "friday",
            "IsDCActive": True
        },
        DemandPlannerHierarchy={
            "DemandPlannerHierarchyNoOfLevels": 4,
            "DemandPlannerHierarchyLevel1": "Demand Planner1",
            "DemandPlannerHierarchyLevel2": "Demand Planner2",
            "DemandPlannerHierarchyLevel3": "Demand Planner3",
            "DemandPlannerHierarchyLevel4": "Demand Planner4"
        },
        SalesmanHierarchy={
            "SalesmanHierarchyNoOfLevels": 4,
            "SalesmanHierarchyLevel1": "ASM",
            "SalesmanHierarchyLevel2": "RSM",
            "SalesmanHierarchyLevel3": "ZSM",
            "SalesmanHierarchyLevel4": "NSM"
        },
    )

    payload = [
        {
            "forecast_header_id_fk": 1,
            "unconstrained":
                [
                    {
                        "forecast_detail_id": 9,
                        "volume": 88
                    },
                    {
                        "forecast_detail_id": 10,
                        "volume": 223
                    },
                    {
                        "forecast_detail_id": 11,
                        "volume": 224
                    }
                ]
        },
        {
            "forecast_header_id_fk": 2,
            "unconstrained":
                [
                    {
                        "forecast_detail_id": 12,
                        "volume": 226
                    },
                    {
                        "forecast_detail_id": 13,
                        "volume": 227
                    },
                    {
                        "forecast_detail_id": 14,
                        "volume": 228
                    }

                ]
        }

    ]

    url = reverse('forecast_review_Unconstrained')
    header = {'HTTP_EMAIL': 'test1@gmail.com', 'HTTP_SNOP_ID': '1'}
    param = {'HTTP_TENANT_ID': '1'}
    response = client.put(url, data=json.dumps(payload), content_type="application/json", **header, **param)
    print('response', response)
    data = response.data
    print("data", data)

    assert data['Success'] == 'Forecast unconstrained review updated'
    assert response.status_code == status.HTTP_200_OK


@pytest.mark.django_db
@mock.patch('app.snop_config.snop.get_confiurations')
def test_forecast_unconstrained_edit_invalid_for_wrong_data(mock_get_confiurations, snop, forecast_header,
                                                            forecast_detail, forecast_approval, forecast_personnel,
                                                            forecast_demandplannerheirarchy, forecast_SalesmanHeirarchy,
                                                            client):
    mock_get_confiurations.return_value = dict(
        snopconfiguration={
            "planningCycleFrequency": "quarterly",
            "planningHorizon": 4,
            "Actualsales": "daily",
            "Granularity": "daily",
            "weekStart": "friday",
            "IsDCActive": True
        },
        DemandPlannerHierarchy={
            "DemandPlannerHierarchyNoOfLevels": 4,
            "DemandPlannerHierarchyLevel1": "Demand Planner1",
            "DemandPlannerHierarchyLevel2": "Demand Planner2",
            "DemandPlannerHierarchyLevel3": "Demand Planner3",
            "DemandPlannerHierarchyLevel4": "Demand Planner4"
        },
        SalesmanHierarchy={
            "SalesmanHierarchyNoOfLevels": 4,
            "SalesmanHierarchyLevel1": "ASM",
            "SalesmanHierarchyLevel2": "RSM",
            "SalesmanHierarchyLevel3": "ZSM",
            "SalesmanHierarchyLevel4": "NSM"
        },
    )

    payload = [
        {
            "forecast_header_id_fk": 1,
            "sales":
                [
                    {
                        "forecast_detail_id": 9,
                        "volume": "88"
                    },
                    {
                        "forecast_detail_id": 10,
                        "volume": 223
                    },
                    {
                        "forecast_detail_id": 11,
                        "volume": 224
                    }
                ]
        },
        {
            "forecast_header_id_fk": 2,
            "unconstrained":
                [
                    {
                        "forecast_detail_id": 12,
                        "volume": 226
                    },
                    {
                        "forecast_detail_id": 13,
                        "volume": 227
                    },
                    {
                        "forecast_detail_id": 14,
                        "volume": 228
                    }

                ]
        }

    ]

    url = reverse('forecast_review_Unconstrained')
    header = {'HTTP_EMAIL': 'test1@gmail.com', 'HTTP_SNOP_ID': '1'}
    param = {'HTTP_TENANT_ID': '1'}
    response = client.put(url, data=json.dumps(payload), content_type="application/json", **header, **param)
    print('response', response)
    data = response.data
    print("data", data)

    assert response.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
@mock.patch('app.snop_config.snop.get_confiurations')
def test_forecast_unconstrained_edit_invalid_volume(mock_get_confiurations, snop, forecast_header, forecast_detail,
                                                    forecast_approval, forecast_personnel,
                                                    forecast_demandplannerheirarchy, forecast_SalesmanHeirarchy,
                                                    client):
    mock_get_confiurations.return_value = dict(
        snopconfiguration={
            "planningCycleFrequency": "quarterly",
            "planningHorizon": 3,
            "Actualsales": "daily",
            "Granularity": "daily",
            "weekStart": "friday",
            "IsDCActive": True
        },
        DemandPlannerHierarchy={
            "DemandPlannerHierarchyNoOfLevels": 4,
            "DemandPlannerHierarchyLevel1": "Demand Planner1",
            "DemandPlannerHierarchyLevel2": "Demand Planner2",
            "DemandPlannerHierarchyLevel3": "Demand Planner3",
            "DemandPlannerHierarchyLevel4": "Demand Planner4"
        },
        SalesmanHierarchy={
            "SalesmanHierarchyNoOfLevels": 4,
            "SalesmanHierarchyLevel1": "ASM",
            "SalesmanHierarchyLevel2": "RSM",
            "SalesmanHierarchyLevel3": "ZSM",
            "SalesmanHierarchyLevel4": "NSM"
        },
    )

    payload = [
        {
            "forecast_header_id_fk": 1,
            "unconstrained":
                [
                    {
                        "forecast_detail_id": 9,
                        "volume": 88
                    },
                    {
                        "forecast_detail_id": 10,
                        "volume": -223
                    },
                    {
                        "forecast_detail_id": 11,
                        "volume": 224
                    }
                ]
        },
        {
            "forecast_header_id_fk": 2,
            "unconstrained":
                [
                    {
                        "forecast_detail_id": 12,
                        "volume": 226
                    },
                    {
                        "forecast_detail_id": 13,
                        "volume": 227
                    },
                    {
                        "forecast_detail_id": 14,
                        "volume": 228
                    }

                ]
        }

    ]

    url = reverse('forecast_review_Unconstrained')
    header = {'HTTP_EMAIL': 'test1@gmail.com', 'HTTP_SNOP_ID': '1'}
    param = {'HTTP_TENANT_ID': '1'}
    response = client.put(url, data=json.dumps(payload), content_type="application/json", **header, **param)
    print('response', response)
    data = response.data
    print("data", data)

    assert data['Error'] == 'Volume needs to match planning horizon & it should be positive'
    assert response.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
@mock.patch('app.snop_config.snop.get_confiurations')
def test_forecast_unconstrained_edit_invalid_planningHorizon(mock_get_confiurations, snop, forecast_header,
                                                             forecast_detail, forecast_approval, forecast_personnel,
                                                             forecast_demandplannerheirarchy,
                                                             forecast_SalesmanHeirarchy, client):
    mock_get_confiurations.return_value = dict(
        snopconfiguration={
            "planningCycleFrequency": "quarterly",
            "planningHorizon": 4,
            "Actualsales": "daily",
            "Granularity": "daily",
            "weekStart": "friday",
            "IsDCActive": True
        },
        DemandPlannerHierarchy={
            "DemandPlannerHierarchyNoOfLevels": 4,
            "DemandPlannerHierarchyLevel1": "Demand Planner1",
            "DemandPlannerHierarchyLevel2": "Demand Planner2",
            "DemandPlannerHierarchyLevel3": "Demand Planner3",
            "DemandPlannerHierarchyLevel4": "Demand Planner4"
        },
        SalesmanHierarchy={
            "SalesmanHierarchyNoOfLevels": 4,
            "SalesmanHierarchyLevel1": "ASM",
            "SalesmanHierarchyLevel2": "RSM",
            "SalesmanHierarchyLevel3": "ZSM",
            "SalesmanHierarchyLevel4": "NSM"
        },
    )

    payload = [
        {
            "forecast_header_id_fk": 1,
            "unconstrained":
                [
                    {
                        "forecast_detail_id": 9,
                        "volume": 88
                    },
                    {
                        "forecast_detail_id": 10,
                        "volume": 223
                    },
                    {
                        "forecast_detail_id": 11,
                        "volume": 224
                    }
                ]
        },
        {
            "forecast_header_id_fk": 2,
            "unconstrained":
                [
                    {
                        "forecast_detail_id": 12,
                        "volume": 226
                    },
                    {
                        "forecast_detail_id": 13,
                        "volume": 227
                    },
                    {
                        "forecast_detail_id": 14,
                        "volume": 228
                    }

                ]
        }

    ]

    url = reverse('forecast_review_Unconstrained')
    header = {'HTTP_EMAIL': 'test1@gmail.com', 'HTTP_SNOP_ID': '1'}
    param = {'HTTP_TENANT_ID': '1'}
    response = client.put(url, data=json.dumps(payload), content_type="application/json", **header, **param)
    print('response', response)
    data = response.data
    print("data", data)

    assert data['Error'] == 'Volume needs to match planning horizon & it should be positive'
    assert response.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
@mock.patch('app.snop_config.snop.get_confiurations')
def test_forecast_unconstrained_edit_invalid_user_email(mock_get_confiurations, snop, forecast_header, forecast_detail,
                                                        forecast_approval, forecast_personnel,
                                                        forecast_demandplannerheirarchy, forecast_SalesmanHeirarchy,
                                                        client):
    mock_get_confiurations.return_value = dict(
        snopconfiguration={
            "planningCycleFrequency": "quarterly",
            "planningHorizon": 3,
            "Actualsales": "daily",
            "Granularity": "daily",
            "weekStart": "friday",
            "IsDCActive": True
        },
        DemandPlannerHierarchy={
            "DemandPlannerHierarchyNoOfLevels": 4,
            "DemandPlannerHierarchyLevel1": "Demand Planner1",
            "DemandPlannerHierarchyLevel2": "Demand Planner2",
            "DemandPlannerHierarchyLevel3": "Demand Planner3",
            "DemandPlannerHierarchyLevel4": "Demand Planner4"
        },
        SalesmanHierarchy={
            "SalesmanHierarchyNoOfLevels": 4,
            "SalesmanHierarchyLevel1": "ASM",
            "SalesmanHierarchyLevel2": "RSM",
            "SalesmanHierarchyLevel3": "ZSM",
            "SalesmanHierarchyLevel4": "NSM"
        },
    )

    payload = [
        {
            "forecast_header_id_fk": 1,
            "unconstrained":
                [
                    {
                        "forecast_detail_id": 9,
                        "volume": 88
                    },
                    {
                        "forecast_detail_id": 10,
                        "volume": 223
                    },
                    {
                        "forecast_detail_id": 11,
                        "volume": 224
                    }
                ]
        },
        {
            "forecast_header_id_fk": 2,
            "unconstrained":
                [
                    {
                        "forecast_detail_id": 12,
                        "volume": 226
                    },
                    {
                        "forecast_detail_id": 13,
                        "volume": 227
                    },
                    {
                        "forecast_detail_id": 14,
                        "volume": 228
                    }

                ]
        }

    ]

    url = reverse('forecast_review_Unconstrained')
    header = {'HTTP_EMAIL': 'test@gmail.com', 'HTTP_SNOP_ID': '1'}
    param = {'HTTP_TENANT_ID': '1'}
    response = client.put(url, data=json.dumps(payload), content_type="application/json", **header, **param)
    print('response', response)
    data = response.data
    print("data", data)

    assert data['Error'] == 'Invalid user identified in email verification'
    assert response.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
@mock.patch('app.snop_config.snop.get_confiurations')
def test_forecast_unconstrained_edit_invalid_approved_till_level(mock_get_confiurations, snop, forecast_header,
                                                                 forecast_detail, forecast_approval_invalid,
                                                                 forecast_personnel, forecast_demandplannerheirarchy,
                                                                 forecast_SalesmanHeirarchy, client):
    mock_get_confiurations.return_value = dict(
        snopconfiguration={
            "planningCycleFrequency": "quarterly",
            "planningHorizon": 3,
            "Actualsales": "daily",
            "Granularity": "daily",
            "weekStart": "friday",
            "IsDCActive": True
        },
        DemandPlannerHierarchy={
            "DemandPlannerHierarchyNoOfLevels": 4,
            "DemandPlannerHierarchyLevel1": "Demand Planner1",
            "DemandPlannerHierarchyLevel2": "Demand Planner2",
            "DemandPlannerHierarchyLevel3": "Demand Planner3",
            "DemandPlannerHierarchyLevel4": "Demand Planner4"
        },
        SalesmanHierarchy={
            "SalesmanHierarchyNoOfLevels": 4,
            "SalesmanHierarchyLevel1": "ASM",
            "SalesmanHierarchyLevel2": "RSM",
            "SalesmanHierarchyLevel3": "ZSM",
            "SalesmanHierarchyLevel4": "NSM"
        },
    )

    payload = [
        {
            "forecast_header_id_fk": 1,
            "unconstrained":
                [
                    {
                        "forecast_detail_id": 9,
                        "volume": 88
                    },
                    {
                        "forecast_detail_id": 10,
                        "volume": 223
                    },
                    {
                        "forecast_detail_id": 11,
                        "volume": 224
                    }
                ]
        },
        {
            "forecast_header_id_fk": 2,
            "unconstrained":
                [
                    {
                        "forecast_detail_id": 12,
                        "volume": 226
                    },
                    {
                        "forecast_detail_id": 13,
                        "volume": 227
                    },
                    {
                        "forecast_detail_id": 14,
                        "volume": 228
                    }

                ]
        }

    ]

    url = reverse('forecast_review_Unconstrained')
    header = {'HTTP_EMAIL': 'test1@gmail.com', 'HTTP_SNOP_ID': '1'}
    param = {'HTTP_TENANT_ID': '1'}
    response = client.put(url, data=json.dumps(payload), content_type="application/json", **header, **param)
    print('response', response)
    data = response.data
    print("data", data)

    assert data['Error'] == 'Invalid user level'
    assert response.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
@mock.patch('app.snop_config.snop.get_confiurations')
def test_forecast_unconstrained_edit_invalid_IsDCActive(mock_get_confiurations, snop, forecast_header, forecast_detail,
                                                        forecast_approval, forecast_personnel,
                                                        forecast_demandplannerheirarchy, forecast_SalesmanHeirarchy,
                                                        client):
    mock_get_confiurations.return_value = dict(
        snopconfiguration={
            "planningCycleFrequency": "quarterly",
            "planningHorizon": 3,
            "Actualsales": "daily",
            "Granularity": "daily",
            "weekStart": "friday",
            "IsDCActive": False
        },
        DemandPlannerHierarchy={
            "DemandPlannerHierarchyNoOfLevels": 4,
            "DemandPlannerHierarchyLevel1": "Demand Planner1",
            "DemandPlannerHierarchyLevel2": "Demand Planner2",
            "DemandPlannerHierarchyLevel3": "Demand Planner3",
            "DemandPlannerHierarchyLevel4": "Demand Planner4"
        },
        SalesmanHierarchy={
            "SalesmanHierarchyNoOfLevels": 4,
            "SalesmanHierarchyLevel1": "ASM",
            "SalesmanHierarchyLevel2": "RSM",
            "SalesmanHierarchyLevel3": "ZSM",
            "SalesmanHierarchyLevel4": "NSM"
        },
    )

    payload = [
        {
            "forecast_header_id_fk": 1,
            "unconstrained":
                [
                    {
                        "forecast_detail_id": 9,
                        "volume": 88
                    },
                    {
                        "forecast_detail_id": 10,
                        "volume": 223
                    },
                    {
                        "forecast_detail_id": 11,
                        "volume": 224
                    }
                ]
        },
        {
            "forecast_header_id_fk": 2,
            "unconstrained":
                [
                    {
                        "forecast_detail_id": 12,
                        "volume": 226
                    },
                    {
                        "forecast_detail_id": 13,
                        "volume": 227
                    },
                    {
                        "forecast_detail_id": 14,
                        "volume": 228
                    }

                ]
        }

    ]

    url = reverse('forecast_review_Unconstrained')
    header = {'HTTP_EMAIL': 'test1@gmail.com', 'HTTP_SNOP_ID': '1'}
    param = {'HTTP_TENANT_ID': '1'}
    response = client.put(url, data=json.dumps(payload), content_type="application/json", **header, **param)
    print('response', response)
    data = response.data
    print("data", data)

    assert data['Error'] == 'config_data IsDCActive false'
    assert response.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
@mock.patch('app.snop_config.snop.get_confiurations')
def test_forecast_unconstrained_edit_invalid_SalesmanHierarchyNoOfLevels(mock_get_confiurations, snop, forecast_header,
                                                                         forecast_detail, forecast_approval,
                                                                         forecast_personnel,
                                                                         forecast_demandplannerheirarchy,
                                                                         forecast_SalesmanHeirarchy, client):
    mock_get_confiurations.return_value = dict(
        snopconfiguration={
            "planningCycleFrequency": "quarterly",
            "planningHorizon": 3,
            "Actualsales": "daily",
            "Granularity": "daily",
            "weekStart": "friday",
            "IsDCActive": True
        },
        DemandPlannerHierarchy={
            "DemandPlannerHierarchyNoOfLevels": 4,
            "DemandPlannerHierarchyLevel1": "Demand Planner1",
            "DemandPlannerHierarchyLevel2": "Demand Planner2",
            "DemandPlannerHierarchyLevel3": "Demand Planner3",
            "DemandPlannerHierarchyLevel4": "Demand Planner4"
        },
        SalesmanHierarchy={
            "SalesmanHierarchyNoOfLevels": 5,
            "SalesmanHierarchyLevel1": "ASM",
            "SalesmanHierarchyLevel2": "RSM",
            "SalesmanHierarchyLevel3": "ZSM",
            "SalesmanHierarchyLevel4": "NSM"
        },
    )

    payload = [
        {
            "forecast_header_id_fk": 1,
            "unconstrained":
                [
                    {
                        "forecast_detail_id": 9,
                        "volume": 88
                    },
                    {
                        "forecast_detail_id": 10,
                        "volume": 223
                    },
                    {
                        "forecast_detail_id": 11,
                        "volume": 224
                    }
                ]
        },
        {
            "forecast_header_id_fk": 2,
            "unconstrained":
                [
                    {
                        "forecast_detail_id": 12,
                        "volume": 226
                    },
                    {
                        "forecast_detail_id": 13,
                        "volume": 227
                    },
                    {
                        "forecast_detail_id": 14,
                        "volume": 228
                    }

                ]
        }

    ]

    url = reverse('forecast_review_Unconstrained')
    header = {'HTTP_EMAIL': 'test1@gmail.com', 'HTTP_SNOP_ID': '1'}
    param = {'HTTP_TENANT_ID': '1'}
    response = client.put(url, data=json.dumps(payload), content_type="application/json", **header, **param)
    print('response', response)
    data = response.data
    print("data", data)

    assert data['Error'] == 'SalesmanHierarchyNoOfLevels is not same as sales approved_till_level'
    assert response.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
@mock.patch('app.snop_config.snop.get_confiurations')
def test_forecast_unconstrained_edit_invalid_snop_status(mock_get_confiurations, snop_invalid, forecast_header,
                                                         forecast_detail, forecast_approval, forecast_personnel,
                                                         forecast_demandplannerheirarchy, forecast_SalesmanHeirarchy,
                                                         client):
    mock_get_confiurations.return_value = dict(
        snopconfiguration={
            "planningCycleFrequency": "quarterly",
            "planningHorizon": 3,
            "Actualsales": "daily",
            "Granularity": "daily",
            "weekStart": "friday",
            "IsDCActive": True
        },
        DemandPlannerHierarchy={
            "DemandPlannerHierarchyNoOfLevels": 4,
            "DemandPlannerHierarchyLevel1": "Demand Planner1",
            "DemandPlannerHierarchyLevel2": "Demand Planner2",
            "DemandPlannerHierarchyLevel3": "Demand Planner3",
            "DemandPlannerHierarchyLevel4": "Demand Planner4"
        },
        SalesmanHierarchy={
            "SalesmanHierarchyNoOfLevels": 4,
            "SalesmanHierarchyLevel1": "ASM",
            "SalesmanHierarchyLevel2": "RSM",
            "SalesmanHierarchyLevel3": "ZSM",
            "SalesmanHierarchyLevel4": "NSM"
        },
    )

    payload = [
        {
            "forecast_header_id_fk": 1,
            "unconstrained":
                [
                    {
                        "forecast_detail_id": 9,
                        "volume": 88
                    },
                    {
                        "forecast_detail_id": 10,
                        "volume": 223
                    },
                    {
                        "forecast_detail_id": 11,
                        "volume": 224
                    }
                ]
        },
        {
            "forecast_header_id_fk": 2,
            "unconstrained":
                [
                    {
                        "forecast_detail_id": 12,
                        "volume": 226
                    },
                    {
                        "forecast_detail_id": 13,
                        "volume": 227
                    },
                    {
                        "forecast_detail_id": 14,
                        "volume": 228
                    }

                ]
        }

    ]

    url = reverse('forecast_review_Unconstrained')
    header = {'HTTP_EMAIL': 'test1@gmail.com', 'HTTP_SNOP_ID': '1'}
    param = {'HTTP_TENANT_ID': '1'}
    response = client.put(url, data=json.dumps(payload), content_type="application/json", **header, **param)
    print('response', response)
    data = response.data
    print("data", data)

    assert data['Error'] == 'SNOP status is NOT in PROGRESS'
    assert response.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
@mock.patch('app.snop_config.snop.get_confiurations')
def test_forecast_unconstrained_edit_invalid_snop_id(mock_get_confiurations, snop, forecast_header, forecast_detail,
                                                     forecast_approval, forecast_personnel,
                                                     forecast_demandplannerheirarchy, forecast_SalesmanHeirarchy,
                                                     client):
    mock_get_confiurations.return_value = dict(
        snopconfiguration={
            "planningCycleFrequency": "quarterly",
            "planningHorizon": 3,
            "Actualsales": "daily",
            "Granularity": "daily",
            "weekStart": "friday",
            "IsDCActive": True
        },
        DemandPlannerHierarchy={
            "DemandPlannerHierarchyNoOfLevels": 4,
            "DemandPlannerHierarchyLevel1": "Demand Planner1",
            "DemandPlannerHierarchyLevel2": "Demand Planner2",
            "DemandPlannerHierarchyLevel3": "Demand Planner3",
            "DemandPlannerHierarchyLevel4": "Demand Planner4"
        },
        SalesmanHierarchy={
            "SalesmanHierarchyNoOfLevels": 4,
            "SalesmanHierarchyLevel1": "ASM",
            "SalesmanHierarchyLevel2": "RSM",
            "SalesmanHierarchyLevel3": "ZSM",
            "SalesmanHierarchyLevel4": "NSM"
        },
    )

    payload = [
        {
            "forecast_header_id_fk": 1,
            "unconstrained":
                [
                    {
                        "forecast_detail_id": 9,
                        "volume": 88
                    },
                    {
                        "forecast_detail_id": 10,
                        "volume": 223
                    },
                    {
                        "forecast_detail_id": 11,
                        "volume": 224
                    }
                ]
        },
        {
            "forecast_header_id_fk": 2,
            "unconstrained":
                [
                    {
                        "forecast_detail_id": 12,
                        "volume": 226
                    },
                    {
                        "forecast_detail_id": 13,
                        "volume": 227
                    },
                    {
                        "forecast_detail_id": 14,
                        "volume": 228
                    }

                ]
        }

    ]

    url = reverse('forecast_review_Unconstrained')
    header = {'HTTP_EMAIL': 'test1@gmail.com', 'HTTP_SNOP_ID': '2'}
    param = {'HTTP_TENANT_ID': '1'}
    response = client.put(url, data=json.dumps(payload), content_type="application/json", **header, **param)
    print('response', response)
    data = response.data
    print("data", data)

    assert data['Error'] == 'SNOP id validation failed'
    assert response.status_code == status.HTTP_400_BAD_REQUEST

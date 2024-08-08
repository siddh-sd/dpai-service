import json
import pytest
from rest_framework.reverse import reverse
from unittest import mock
from rest_framework import status


@pytest.mark.django_db
@mock.patch('app.snop_config.snop.get_confiurations')
def test_forecast_detail_edit_valid(mock_get_confiurations, snop, forecast_header, forecast_detail, forecast_approval, forecast_personnel, forecast_demandplannerheirarchy, client):
    mock_get_confiurations.return_value = dict(snopconfiguration={
        "planningCycleFrequency": "quarterly",
        "planningHorizon": 4,
        "Actualsales": "daily",
        "Granularity": "daily",
        "weekStart": "friday"
    })

    payload = [
        {
            "forecast_header_id_fk": 1,
            "Operational":
                [
                    {
                        "forecast_detail_id": 1,
                        "volume": 88
                    },
                    {
                        "forecast_detail_id": 2,
                        "volume": 223
                    },
                    {
                        "forecast_detail_id": 3,
                        "volume": 224
                    },
                    {
                        "forecast_detail_id": 7,
                        "volume": 225
                    }
                ]
        },
        {
            "forecast_header_id_fk": 2,
            "Operational":
                [
                    {
                        "forecast_detail_id": 4,
                        "volume": 226
                    },
                    {
                        "forecast_detail_id": 5,
                        "volume": 227
                    },
                    {
                        "forecast_detail_id": 6,
                        "volume": 228
                    },
                    {
                        "forecast_detail_id": 8,
                        "volume": 123123
                    }
                ]
        }
    ]
    url = reverse('forecast_review_operational')
    header = {'HTTP_EMAIL': 'test1@gmail.com'}
    response = client.put(url, data=json.dumps(payload), content_type="application/json", **header)
    print('response', response)
    data = response.data
    print("data", data)
    assert data['data']['Success'] == 'forecast_detail updated'
    assert response.status_code == status.HTTP_200_OK


@pytest.mark.django_db
@mock.patch('app.snop_config.snop.get_confiurations')
def test_forecast_detail_edit_invalid_data(mock_get_confiurations, snop, forecast_header, forecast_detail, forecast_approval, forecast_personnel, forecast_demandplannerheirarchy, client):

    mock_get_confiurations.return_value = dict(snopconfiguration={
        "planningCycleFrequency": "quarterly",
        "planningHorizon": 4,
        "Actualsales": "daily",
        "Granularity": "daily",
        "weekStart": "friday"
    })

    payload = [
        {
            "forecast_header_id_fk": 1,
            "Operational":
                [
                    {
                        "forecast_detail_id": 1,
                        "volume": "88"
                    },
                    {
                        "forecast_detail_id": 2,
                        "volume": 223
                    },
                    {
                        "forecast_detail_id": 3,
                        "volume": 224
                    },
                    {
                        "forecast_detail_id": 7,
                        "volume": 225
                    }
                ]
        },
        {
            "forecast_header_id_fk": 2,
            "Operational":
                [
                    {
                        "forecast_detail_id": 4,
                        "volume": 226
                    },
                    {
                        "forecast_detail_id": 5,
                        "volume": 227
                    },
                    {
                        "forecast_detail_id": 6,
                        "volume": 228
                    },
                    {
                        "forecast_detail_id": 8,
                        "volume": 123123
                    }
                ]
        }
    ]
    url = reverse('forecast_review_operational')
    header = {'HTTP_EMAIL': 'test1@gmail.com'}
    response = client.put(url, data=json.dumps(payload), content_type="application/json", **header)
    print('response', response)
    data = response.data
    print("data", data)
    # assert data['Success'] == 'forecast_detail updated'
    assert response.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
@mock.patch('app.snop_config.snop.get_confiurations')
def test_forecast_detail_edit_invalid_volume(mock_get_confiurations, snop, forecast_header, forecast_detail, forecast_approval, forecast_personnel, forecast_demandplannerheirarchy, client):

    mock_get_confiurations.return_value = dict(snopconfiguration={
        "planningCycleFrequency": "quarterly",
        "planningHorizon": 4,
        "Actualsales": "daily",
        "Granularity": "daily",
        "weekStart": "friday"
    })

    payload = [
        {
            "forecast_header_id_fk": 1,
            "Operational":
                [
                    {
                        "forecast_detail_id": 1,
                        "volume": -88
                    },
                    {
                        "forecast_detail_id": 2,
                        "volume": 223
                    },
                    {
                        "forecast_detail_id": 3,
                        "volume": 224
                    },
                    {
                        "forecast_detail_id": 7,
                        "volume": 225
                    }
                ]
        },
        {
            "forecast_header_id_fk": 2,
            "Operational":
                [
                    {
                        "forecast_detail_id": 4,
                        "volume": 226
                    },
                    {
                        "forecast_detail_id": 5,
                        "volume": 227
                    },
                    {
                        "forecast_detail_id": 6,
                        "volume": 228
                    },
                    {
                        "forecast_detail_id": 8,
                        "volume": 123123
                    }
                ]
        }
    ]
    url = reverse('forecast_review_operational')
    header = {'HTTP_EMAIL': 'test1@gmail.com'}
    response = client.put(url, data=json.dumps(payload), content_type="application/json", **header)
    print('response', response)
    data = response.data
    print("data", data)
    assert response.status_code == status.HTTP_400_BAD_REQUEST

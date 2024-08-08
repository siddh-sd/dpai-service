import json
import pytest
from rest_framework.reverse import reverse
from unittest import mock
import urllib


@pytest.mark.django_db
@mock.patch('app.snop_config.snop.get_confiurations')
def test_forecast_date_added(mock_get_confiurations, tenant_new1, client):
    mock_get_confiurations.return_value = dict(snopconfiguration={
        "planningCycleFrequency": "daily",
        "planningHorizon": 3,
        "Actualsales": "daily",
        "Granularity": "daily",
        "weekStart": "Thursday",
        "isDCActive": True
    },
    DemandPlannerHierarchy={
            "DemandPlannerHierarchyNoOfLevels": 4,
            "DemandPlannerHierarchyLevel1": "Demand Planner1",
            "DemandPlannerHierarchyLevel2": "Demand Planner2",
            "DemandPlannerHierarchyLevel3": "Demand Planner3",
            "DemandPlannerHierarchyLevel4": "Demand Planner4"
    })

    payload = {
        "snop_name": "g",
        "from_date": "2024-01-01",
        "to_date": "2024-01-03",
        "demand_review_date": "2023-10-05",
        "supply_review_date": "2023-10-06",
        "pre_snop_date": "2023-10-07",
        "snop_date": "2023-10-08"
    }
    url = reverse('snop')
    response = client.post(url+"?tenant_id=1&bu_id=1", data=json.dumps(payload), content_type="application/json")
    data = response.data
    assert response.status_code == 201

    data = {"tenant_id":"1","bu_id":"1","snop_id":"1"}
    url_params = urllib.parse.urlencode(data)
    rev_url = reverse('forecast_header_data')
    url = f"{rev_url}?{url_params}"

    payload = [
    {
        "forecastNumber": "F001",
        "ProductAttribute": "Product1",
        "LocationAttribute": "Location1",
        "Variability": "X",
        "Accuracy": 60,
        "AverageMonthlySalesVolume": 300,
        "LastMonthActualSaleVolume": 20,
        "AverageMonthlySalesValue": 20.10,
        "LastMonthActualSaleValue": 30.10,
        "UpperConfidenceValue": 500.5,
        "LowerConfidenceValue": 200.6,
        "N1Fidelity": 30,
        "N3Fidelity": 50,
        "Statistical": [
            {
                "Period": "Jan 2020",
                "Volume": 500,
                "Value": 1298.67
            },
            {
                "Period": "Feb 2021",
                "Volume": 1500,
                "Value": 1298.67
            },
            {
                "Period": "Mar 2022",
                "Volume": 2500,
                "Value": 1298.67
            }
        ],
        "Segment": [
            {
                "emailAddress": "abc.gmail.com",
                "Value": "A"
            },
            {
                "emailAddress": "abc2.gmail.com",
                "Value": "B"
            },
            {
                "emailAddress": "abc3.gmail.com",
                "Value": "C"
            },
            {
                "emailAddress": "abc4.gmail.com",
                "Value": "D"
            }
        ]
    },
    {
        "forecastNumber": "F002",
        "ProductAttribute": "Product2",
        "LocationAttribute": "Location2",
        "Variability": "Y",
        "Accuracy": 160,
        "AverageMonthlySalesVolume": 1300,
        "LastMonthActualSaleVolume": 120,
        "AverageMonthlySalesValue": 210.10,
        "LastMonthActualSaleValue": 130.10,
        "UpperConfidenceValue": 1500.5,
        "LowerConfidenceValue": 1200.6,
        "N1Fidelity": 130,
        "N3Fidelity": 150,
        "Statistical": [
            {
                "Period": "Apr 2012",
                "Volume": 150,
                "Value": 1298.67
            },
            {
                "Period": "May 2013",
                "Volume": 700,
                "Value": 1298.67
            },
            {
                "Period": "June 2014",
                "Volume": 600,
                "Value": 1298.67
            }
        ],
        "Segment": [
            {
                "emailAddress": "ab.gmail.com",
                "Value": "AB"
            },
            {
                "emailAddress": "xyz.gmail.com",
                "Value": "XYZ"
            },
            {
                "emailAddress": "de.gmail.com",
                "Value": "DE"
            },
            {
                "emailAddress": "fg.gmail.com",
                "Value": "FG"
            }
        ]
    }
]
    response = client.post(url, data=json.dumps(payload), content_type="application/json")
    assert response.data ==  {"responseCode": "SUCCESS_WITH_FORECAST_DATA","responseMessage":"Forecast data added successfully"}
    assert response.status_code == 200


@pytest.mark.django_db
@mock.patch('app.snop_config.snop.get_confiurations')
def test_forecast_snop_closed(mock_get_confiurations, tenant_new1, client):
    mock_get_confiurations.return_value = dict(snopconfiguration={
        "planningCycleFrequency": "daily",
        "planningHorizon": 3,
        "Actualsales": "daily",
        "Granularity": "daily",
        "weekStart": "Thursday",
        "isDCActive": True
    },
    DemandPlannerHierarchy={
            "DemandPlannerHierarchyNoOfLevels": 4,
            "DemandPlannerHierarchyLevel1": "Demand Planner1",
            "DemandPlannerHierarchyLevel2": "Demand Planner2",
            "DemandPlannerHierarchyLevel3": "Demand Planner3",
            "DemandPlannerHierarchyLevel4": "Demand Planner4"
    })

    payload = {
        "snop_name": "g",
        "from_date": "2023-01-01",
        "to_date": "2023-01-03",
        "demand_review_date": "2022-10-05",
        "supply_review_date": "2022-10-06",
        "pre_snop_date": "2022-10-07",
        "snop_date": "2022-10-08"
    }
    url = reverse('snop')
    response = client.post(url+"?tenant_id=1&bu_id=1", data=json.dumps(payload), content_type="application/json")
    data = response.data
    assert response.status_code == 201

    data = {"tenant_id":"1","bu_id":"1","snop_id":"1"}
    url_params = urllib.parse.urlencode(data)
    rev_url = reverse('forecast_header_data')
    url = f"{rev_url}?{url_params}"

    payload = [
    {
        "forecastNumber": "F001",
        "ProductAttribute": "Product1",
        "LocationAttribute": "Location1",
        "Variability": "X",
        "Accuracy": 60,
        "AverageMonthlySalesVolume": 300,
        "LastMonthActualSaleVolume": 20,
        "AverageMonthlySalesValue": 20.10,
        "LastMonthActualSaleValue": 30.10,
        "UpperConfidenceValue": 500.5,
        "LowerConfidenceValue": 200.6,
        "N1Fidelity": 30,
        "N3Fidelity": 50,
        "Statistical": [
            {
                "Period": "Jan 2020",
                "Volume": 500,
                "Value": 1298.67
            },
            {
                "Period": "Feb 2021",
                "Volume": 1500,
                "Value": 1298.67
            },
            {
                "Period": "Mar 2022",
                "Volume": 2500,
                "Value": 1298.67
            }
        ],
        "Segment": [
            {
                "emailAddress": "abc.gmail.com",
                "Value": "A"
            },
            {
                "emailAddress": "abc2.gmail.com",
                "Value": "B"
            },
            {
                "emailAddress": "abc3.gmail.com",
                "Value": "C"
            },
            {
                "emailAddress": "abc4.gmail.com",
                "Value": "D"
            }
        ]
    },
    {
        "forecastNumber": "F002",
        "ProductAttribute": "Product2",
        "LocationAttribute": "Location2",
        "Variability": "Y",
        "Accuracy": 160,
        "AverageMonthlySalesVolume": 1300,
        "LastMonthActualSaleVolume": 120,
        "AverageMonthlySalesValue": 210.10,
        "LastMonthActualSaleValue": 130.10,
        "UpperConfidenceValue": 1500.5,
        "LowerConfidenceValue": 1200.6,
        "N1Fidelity": 130,
        "N3Fidelity": 150,
        "Statistical": [
            {
                "Period": "Apr 2012",
                "Volume": 150,
                "Value": 1298.67
            },
            {
                "Period": "May 2013",
                "Volume": 700,
                "Value": 1298.67
            },
            {
                "Period": "June 2014",
                "Volume": 600,
                "Value": 1298.67
            }
        ],
        "Segment": [
            {
                "emailAddress": "ab.gmail.com",
                "Value": "AB"
            },
            {
                "emailAddress": "xyz.gmail.com",
                "Value": "XYZ"
            },
            {
                "emailAddress": "de.gmail.com",
                "Value": "DE"
            },
            {
                "emailAddress": "fg.gmail.com",
                "Value": "FG"
            }
        ]
    }
]
    response = client.post(url, data=json.dumps(payload), content_type="application/json")
    assert response.data ==  {"responseCode": "SNOP_CLOSED","responseMessage": "Snop is closed"}
    assert response.status_code == 400
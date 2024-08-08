import pytest
from rest_framework.test import APIClient
from datetime import datetime
# from app.models import Snop, ForecastDetail, ForecastHeader, ForecastApproval, Personnel, DemandPlannerHierarchy, SalesmanHeirarchy
# from app.models import Client
from app.models import Client
from snop.models import Snop
from app.model.forecast.forecast_approval import ForecastApproval
from app.model.forecast.forecast_detail import ForecastDetail
from app.model.forecast.forecast_header import ForecastHeader
# Personnel, DemandPlannerHierarchy,SalesmanHeirarchy
# from app.serializers.tenant.tenant_create import TenantSerializer


@pytest.fixture(scope="function")
def client():
    yield APIClient()


@pytest.fixture(scope="function")
def snop():
    snop_obj = Snop.objects.create(
        snop_id="b1935f4d-9929-4e58-b712-d3382d404d60", snop_name="sample", from_date="2023-01-31",
        to_date="2023-02-02",
        demand_review_date="2023-01-07", supply_review_date="2023-01-08", pre_snop_date="2023-01-09",
        snop_date="2023-01-13", forecast_trigger_date="2023-01-06", bu_id="0ba06f15-a525-4588-93f2-40c0df757438",
        updated_at=datetime.now())

    yield snop_obj


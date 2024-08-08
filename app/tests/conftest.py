import pytest
from rest_framework.test import APIClient
from datetime import datetime
from snop.models import Snop
from app.model.forecast.forecast_approval import ForecastApproval
from app.model.forecast.forecast_detail import ForecastDetail
from app.model.forecast.forecast_header import ForecastHeader



@pytest.fixture(scope="function")
def client():
    yield APIClient()


@pytest.fixture(scope="function")
def snop():
    snop_obj = Snop.objects.create(
        snop_id="b1935f4d-9929-4e58-b712-d3382d404d60", snop_name="sample", from_date="2023-01-31",
        to_date="2023-02-02",
        demand_review_date="2023-01-07", supply_review_date="2023-01-08", pre_snop_date="2023-01-09",
        snop_date="2023-01-17", forecast_trigger_date="2023-01-06", bu_id="0ba06f15-a525-4588-93f2-40c0df757438",
        updated_at=datetime.now())

    yield snop_obj


# @pytest.fixture(scope="function")
# def tenant():
#     # tenant_obj = Client.objects.create(tenant_name="abc", schema_name="abc")
#     tenant_obj = Client.objects.create(tenant_id="1e8b88e2-10e4-43b1-a0f8-96cbafe1ccd3", schema_name="abc")
#     yield tenant_obj

# @pytest.fixture(scope="function")
# def tenant_new1():
#     serializer = TenantSerializer(data={"tenant_id":"e7373db9-80bf-4f8d-864f-6f871105f6c3"})
#     if serializer.is_valid():
#         serializer.save()
#         yield serializer.data
#     else:
#         yield True

# @pytest.fixture(scope="function")
# def tenant():
#     # tenant_obj = Client.objects.create(schema_name="scai_dpai_1")
#     tenant_id = "e7373db9-80bf-4f8d-864f-6f871105f6c3"
#     schema = "scai_dpai_" + (tenant_id).replace("-", "_")
#     tenant_obj=Client.objects.create(schema_name=schema)
#     yield tenant_obj

# @pytest.fixture(scope="function")
# def tenant_get():
#     # tenant_obj = Client.objects.create(schema_name="scai_dpai_1")
#     tenant_id = "e7373db9-80bf-4f8d-864f-6f871105f6c3"
#     schema = "abc_" + (tenant_id).replace("-", "_")
#     tenant_obj=Client.objects.create(schema_name=schema)
#     yield tenant_obj


@pytest.fixture(scope="function")
def forecast_header():
    snop_id = Snop.objects.get(snop_id="b1935f4d-9929-4e58-b712-d3382d404d60")
    forecast_header_obj = ForecastHeader.objects.create(forecast_header_id=1, ForecastNumber=15, snop_id_fk=snop_id,
                                                        variability="var", accuracy=12,
                                                        average_monthly_sales_volume=15,
                                                        last_month_actual_sales_volume=13, product_attribute="SKU10",
                                                        location_attribute="Node10", average_monthly_sales_value=21,
                                                        last_month_actual_sales_value=23,
                                                        upper_confidence_value=45, lower_confidence_value=5,
                                                        n1_fidelity=34, n3_fidelity=43,
                                                        updated_at=datetime.now())

    yield forecast_header_obj


@pytest.fixture(scope="function")
def forecast_header_sales():
    snop_id = Snop.objects.get(snop_id="b1935f4d-9929-4e58-b712-d3382d404d60")
    forecast_header_obj = ForecastHeader.objects.create(forecast_header_id=1, ForecastNumber=15, snop_id_fk=snop_id,
                                                        variability="var", accuracy=12,
                                                        average_monthly_sales_volume=15,
                                                        last_month_actual_sales_volume=13, product_attribute="SKU3",
                                                        location_attribute="Node3", average_monthly_sales_value=21,
                                                        last_month_actual_sales_value=23,
                                                        upper_confidence_value=45, lower_confidence_value=5,
                                                        n1_fidelity=34, n3_fidelity=43,
                                                        updated_at=datetime.now())

    yield forecast_header_obj


@pytest.fixture(scope="function")
def forecast_detail():
    header_id = ForecastHeader.objects.get(forecast_header_id=1)

    forecast_detail_obj = ForecastDetail.objects.bulk_create([
        ForecastDetail(forecast_detail_id=5, forecast_header_id_fk=header_id, forecast_type="operational", period="2023-05-01",
                       volume=77,
                       value=None, updated_at=datetime.now()),

        ForecastDetail(forecast_detail_id=6, forecast_header_id_fk=header_id, forecast_type="operational", period="2023-05-01",
                       volume=78,
                       value=None, updated_at=datetime.now()),

        ForecastDetail(forecast_detail_id=7, forecast_header_id_fk=header_id, forecast_type="operational", period="2023-05-01",
                       volume=79,
                       value=None, updated_at=datetime.now()),

        ForecastDetail(forecast_detail_id=8, forecast_header_id_fk=header_id, forecast_type="sales", period="2023-05-01",
                       volume=79,
                       value=None, updated_at=datetime.now()),

        ForecastDetail(forecast_detail_id=9, forecast_header_id_fk=header_id, forecast_type="sales", period="2023-05-01",
                       volume=79,
                       value=None, updated_at=datetime.now()),

        ForecastDetail(forecast_detail_id=10, forecast_header_id_fk=header_id, forecast_type="sales", period="2023-05-01",
                       volume=79,
                       value=None, updated_at=datetime.now()),

    ])
    yield forecast_detail_obj


@pytest.fixture(scope="function")
def forecast_approval():
    header_id = ForecastHeader.objects.get(forecast_header_id=1)
    forecast_approval_obj = ForecastApproval.objects.bulk_create([
        ForecastApproval(forecast_approval_id=1, forecast_header_id_fk=header_id, forecast_type="operational",
                         approved_till_level=0, updated_at=datetime.now()),

        ForecastApproval(forecast_approval_id=2, forecast_header_id_fk=header_id, forecast_type="operational",
                         approved_till_level=0, updated_at=datetime.now()),

        ForecastApproval(forecast_approval_id=3, forecast_header_id_fk=header_id, forecast_type="sales",
                         approved_till_level=3, updated_at=datetime.now())

    ])
    yield forecast_approval_obj


@pytest.fixture(scope="function")
def forecast_approval_sales():
    header_id = ForecastHeader.objects.get(forecast_header_id=1)
    forecast_approval_obj = ForecastApproval.objects.bulk_create([
        ForecastApproval(forecast_approval_id=1, forecast_header_id_fk=header_id, forecast_type="operational",
                         approved_till_level=3, updated_at=datetime.now()),

        ForecastApproval(forecast_approval_id=2, forecast_header_id_fk=header_id, forecast_type="operational",
                         approved_till_level=0, updated_at=datetime.now()),

        ForecastApproval(forecast_approval_id=3, forecast_header_id_fk=header_id, forecast_type="sales",
                         approved_till_level=0, updated_at=datetime.now())

    ])
    yield forecast_approval_obj


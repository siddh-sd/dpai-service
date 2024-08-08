import json
import pytest
from rest_framework.reverse import reverse


@pytest.mark.django_db
def test_tenant_create_valid(client):
    payload = {
        "tenant_id": "456"
    }
    url = reverse('tenant')
    response = client.post(url, data=json.dumps(payload), content_type="application/json")
    data = response.data
    assert data["Successes"] == 'Tenant Created'
    assert data['data']["tenant_id"] == payload['tenant_id']
    assert response.status_code == 201


@pytest.mark.django_db
def test_tenant_avoid_duplicate_data_valid(tenant, client):

    payload = {
        "tenant_id": "345"
    }
    url = reverse('tenant')
    response = client.post(url, data=json.dumps(payload), content_type="application/json")
    data = response.data
    assert data["tenant_id"][0] == "client with this tenant name already exists."
    assert data["schema_name"][0] == "client with this schema name already exists."
    assert response.status_code == 400

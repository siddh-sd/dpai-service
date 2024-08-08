from django.urls import path
from tenant.views import TenantController

urlpatterns = [
    path('tenant', TenantController.as_view(), name="tenant"),
]

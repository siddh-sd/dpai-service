from django.urls import path
from snop.views.snop import Snop
from snop.views.defaultSnop import DefaultSnop

urlpatterns = [
    path('', Snop.as_view(), name="snop"),
    path('default', DefaultSnop.as_view(), name="default_snop")
]

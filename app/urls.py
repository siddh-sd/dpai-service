from django.urls import path
from app.views.forecast.forecast import Forecast, ForecastXLSX, ForecastFilter, ForecastNetwork
from app.views.forecast.adjustment.adjustment import ForecastAdjustment
from app.views.forecast.review.approve import ForecastApprove
from app.views.forecast.review.reject import ForecastReject
from app.views.analytics.analytics import Analytics
from app.views.marketing.activate import Activate
from app.views.marketing.promotionFilter import PromotionFilter
from app.views.marketing.promotion import Promotion
from app.views.marketing.promotion_method import PromotionMethod
from app.views.simulation.scenario import Scenario, DuplicateScenario
from app.views.simulation.scenario_output import ScenarioOutput
from app.views.simulation.groupFilter import GroupFilter
from app.views.simulation.case_compare import CaseCompare
from app.views.simulation.case import Cases , DuplicateCase, RunCase, FreezeCase
from app.views.simulation.simulate import Simulate
from app.views.forecast.alert import Alert

urlpatterns = [
    #Foreccasting & Collaboration
    path('forecast/<str:forecast_type>', Forecast.as_view(), name="forecast"),
    path('forecast/<str:forecast_type>/xlsx', ForecastXLSX.as_view(), name="forecast_xlsx"),
    path('forecast/<str:forecast_type>/approve', ForecastApprove.as_view(), name="forecast_approve"),
    path('forecast/<str:forecast_type>/reject', ForecastReject.as_view(), name="forecast_reject"),
    path('forecast/<str:forecast_type>/adjustment', ForecastAdjustment.as_view(), name="forecast_adjustment"),
    path('forecast/<str:forecast_type>/alert', Alert.as_view(), name="forecast_alert"),
    path('filter', ForecastFilter.as_view(), name="forecast_filter"),
    path('network', ForecastNetwork.as_view(), name="forecast_network"),

    #Analytics
    path('analytics', Analytics.as_view(), name="analytics"),
    
    #Simulation
    path('simulate/scenario', Scenario.as_view(), name='scenario_api'),
    path('simulate/scenario/groupFilter', GroupFilter.as_view(), name="group_filter"),
    path('simulate/scenario/output', ScenarioOutput.as_view(), name="scenario_output"),
    path('simulate/scenario/case',Cases.as_view(),name="scenario-case-api"),
    path('simulate/scenario/duplicate', DuplicateScenario.as_view(), name='duplicate_scenario_api'),
    path('simulate/scenario/case/compare', CaseCompare.as_view(), name="scenario-case-compare"),
    path('simulate/scenario/case/duplicate', DuplicateCase.as_view(), name='duplicate_case_api'),
    path('simulate/scenario/case/run', RunCase.as_view(), name='run_case_api'),
    path('simulate', Simulate.as_view(), name='simulate'),
    path('simulate/scenario/case/freeze', FreezeCase.as_view(), name='freeze_case_api'),

    #Marketing
    path('marketing/promotion/filter', PromotionFilter.as_view(), name="promotion_filter"),
    path('marketing/promotion', Promotion.as_view(), name='promotion_api'),
    path('marketing/promotion/activate', Activate.as_view(), name='promotion_activate_api'),
    path('marketing/promotion/duplicate', PromotionMethod.as_view(), name='promotion_duplicate_api'),
]

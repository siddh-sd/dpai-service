from enum import Enum

class SnopStatus(Enum):
    IN_PROGRESS = 1
    PLANNED = 2
    PLANNED_EXECUTED = 3

class ResponseCodes(Enum):
    SNOP_GET_SUCCESS = 1
    SNOP_GET_FAILED = 2
    SNOP_CREATE_SUCCESS = 3
    SNOP_CREATE_FAILED = 4
    SNOP_INVALID_CONFIGURATIONS = 5
    SNOP_INVALID_ALREADY_EXIST_FROM_DATE = 6
    SNOP_INVALID_DAILY_HORIZON = 7
    SNOP_INVALID_WEEKLY_HORIZON_WEEK_START = 8
    SNOP_INVALID_WEEKLY_HORIZON = 9
    SNOP_INVALID_FORTNIGHT_HORIZON = 10
    SNOP_INVALID_MONTHLY_HORIZON = 11
    SNOP_INVALID_MONTHLY_HORIZON_MONTH_START = 12
    SNOP_INVALID_QUATERLY_HORIZON = 13
    SNOP_INVALID_PRESNOP_DATE = 14
    SNOP_INVALID_SUPPLY_REVIEW_DATE = 15
    SNOP_INVALID_DEMAND_REVIEW_DATE = 16
    SNOP_INVALID_SNOP_DATE = 17
    SNOP_INVALID_FROM_DATE = 18
    SNOP_INVALID_FORECAST_TRIGGER_DATE = 19
    UNAUTHORIZED = 20
    SNOP_INVALID_FROM_DATE_TO_DATE_MODIFICATION = 21
    SNOP_UPDATE_SUCCESS = 22
    SNOP_UPDATE_FAILED = 23
    SNOP_DELETE_SUCCESS = 24
    SNOP_DELETE_FAILED = 25

class ConfigurationKeys(Enum):
    snop = 1

class SnopConfigurationKeys(Enum):
    weekStart = 1
    planningCycleFrequency = 2
    planningHorizon = 3
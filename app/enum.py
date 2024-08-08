from enum import Enum


class SnopStatus(Enum):
    IN_PROGRESS = 1
    PLANNED = 2
    PLANNED_EXECUTED = 3


class SnopConfigurationKeys(Enum):
    snop = 1
    planningHorizon = 2
    planningCycleFrequency = 3


class EntitiesConfigurationKeys(Enum):
    demandPlannerHierarchy = 1
    demandPlannerHierarchyNoOfLevels = 2
    sku_hierarchy_attribute = 3
    location_hierarchy_attribute = 4
    salesmanHierarchyLevel = 5
    demandPlannerHierarchyLevel = 6
    salesmanHierarchy = 7
    salesmanHierarchyNoOfLevels = 8
    productHierarchy = 9
    productHierarchyNoOfLevels = 10
    productHierarchyLevel = 11
    locationHierarchy = 12
    locationHierarchyNoOfLevels = 13
    locationHierarchyLevel = 14
    emaildemandPlannerHierarchyLevel = 15
    emailsalesmanHierarchyLevel = 16
    entities = 17
    base = 18

class ForecastConfigurationKeys(Enum):
    ABCClassificationType = 1
    forecast = 2
    productForecastLevel = 3
    locationForecastLevel = 4
    isDCActive = 5
    salesHistoryConsideration = 6
    forecastChannels = 7
    forecastLocationTypes = 8
    definitionClassificationA = 9
    definitionClassificationB = 10
    sparsityHigh = 11
    sparsityMedium = 12
    salesHistoryDisaggregationConsideration = 13
    NPIDays = 14
    toleranceLimit = 15
    extraColumns = 16
    location = 17
    product = 18
    definition = 19
    useFeaturesInForecast = 20
    forecastComparisonLastCycle = 21

class SegmentationType(Enum):
    STATIC = 1
    DYNAMIC = 2


class ForecastType(Enum):
    STATISTICAL = 1
    OPERATIONAL = 2
    SALES = 3
    UNCONSTRAINED = 4

class AbbreviatedForecastType(Enum):
    s = 1
    o = 2
    sa = 3
    u = 4
    
class ForecastStatus(Enum):
    OPEN = 1
    REVIEWED = 2
    CLOSED = 3


class ResponseCodes(Enum):
    FORECAST_UPDATE_FAILED = 1
    UNAUTHORIZED = 2
    FORECASTING_NOT_ENABLED = 3
    PERSONNEL_DATA_NOT_AVAILABLE = 4
    INVALID_REQUEST = 5
    SNOP_INACTIVE = 6
    FORECAST_UPDATE_SUCCESS = 7
    PRODUCT_DATA_NOT_AVAILABLE = 8
    LOCATION_DATA_NOT_AVAILABLE = 9
    FORECAST_DATA_NOT_AVAILABLE = 10
    SALES_HISTORY_DATA_NOT_AVAILABLE = 11
    FORECAST_GET_SUCCESS = 12
    FORECAST_APPROVE_SUCCESS = 13
    FORECAST_ADJUST_SUCCESS = 14
    FORECAST_REJECT_SUCCESS = 15
    SCENARIO_GET_SUCCESS = 16
    SCENARIO_CREATED_SUCCESS = 17
    SCENARIO_GET_FAILED = 18
    SCENARIO_DATA_NOT_AVAILABLE = 19
    SCENARIO_GROUP_FILTER_FAILED = 20
    SCENARIO_GROUP_FILTER_SUCCESS = 21
    SCENARIO_OUTPUT_SUCCESS = 22
    FORECAST_CREATE_FAILED = 23
    SIMULATE_CASE_COMPARISON_SUCCESS = 24
    SIMULATION_TRIGGERED_SUCCESSFULLY = 25
    ALERT_DATA_NOT_AVAILABLE = 26
    ALERT_GET_SUCCESS = 27
    FORECAST_NUMBER_DATA_ALREADY_RECEIVED = 28
    ADJUSTMENT_LOG_GET_SUCCESS = 29
    FORECAST_CREATE_SUCCESS = 30
    SIMULATION_CREATE_SUCCESS = 31
    SIMULATION_NUMBER_DATA_ALREADY_RECEIVED = 32
    SIMULATION_CREATE_FAILED = 33
    PROMOTION_FILTER_SUCCESS = 34
    PROMOTION_FILTER_FAILED = 35
    PROMOTION_GET_SUCCESS = 36
    PROMOTION_GET_FAILED = 37
    PROMOTION_ACTIVATE_STATUS_CHANGED_SUCCESS = 38
    PROMOTION_CREATED_SUCCESS = 39
    FORECAST_GET_FILTER_SUCCESS = 40
    FORECAST_GET_NETWORK_SUCCESS = 41
    PROMOTION_DELETE_FAILED = 42
    PROMOTION_DELETE_SUCCESS = 43
    ANALYTICS_GET_SUCCESS = 44
    SCENARIO_OUTPUT_COMBINATION_SUCCESS = 45

class PersonnelAttributeNames(Enum):
    sku_hierarchy_attribute = 1
    location_hierarchy_attribute = 2
    sku_id = 3
    id = 4
    node_id = 5
    channel = 6
    channel_id = 7
    sku_name = 8
    channel_name = 9
    node = 10


class NetworkAttributeNames(Enum):
    sku = 1
    start_node = 2
    end_node = 3
    id = 4
    channel = 5
    channel_id = 6

class ProductAttributeNames(Enum):
    id = 1
    sku_name = 2
    sku_code = 3
    channel = 4
    channel_name = 5
    channel_id = 6
    mapping_id = 7
    lifecycle_start_date = 8
    product_lifecycle_status = 9
    lifecycle_end_date = 10
    like_sku = 11
    like_sku_id = 12
    unit_price = 13

class LocationAttributeNames(Enum):
    id = 1
    node = 2
    node_type = 3
    node_type_title = 4
    node_code = 5
    coordinates = 6
    node_mapping_id = 7
    node_lifecycle_Status = 8
    node_lifecycle_end_date = 9

class AdjustmentOperations(Enum):
    PERCENTINC = 1
    PERCENTDEC = 2
    ABSINC = 3
    ABSDEC = 4

class PlanningFrequencies(Enum):
    DAILY = 1
    FORTNIGHTLY = 2
    WEEKLY = 3
    MONTHLY = 4
    QUATERLY = 5

class SalesHistoryAttributes(Enum):
    sku = 1
    node = 2
    date = 3
    salesman_email_address = 4
    actual_sales_volume = 5
    actual_sales_value = 6
    id = 7
    channel = 8
    channel_name = 9
    channel_id = 10
    sku_id = 11
    node_id = 12

class DataScienceGetForecastAttributes(Enum):
    status = 1
    result = 2
    forecast_file_path = 3
    key = 4
    forecast = 5
    period = 6
    id = 7
    product_level_name = 8
    location_level_name = 9
    channel_level_name = 10
    error = 11
    model = 12
    date = 13
    value = 14
    product_id = 15
    location_id = 16
    channel_id = 17
    classification_file_path = 18
    abc_class = 19
    model_profile_bag_file_path = 20
    sparsity = 21
    is_seasonal = 22
    variation = 23
    cv2 = 24
    adi = 25
    fmr_class = 26

class DataScienceForecastStatus(Enum):
    SUCCESS = 1
    PENDING = 2
    RECEIVED = 3
    IN_PROGRESS = 4
    FAILED = 5

class MappingAttributes(Enum):
    sku = 1
    node = 2
    id = 3
    channel = 4
    channel_id = 5
    mapping_life_cycle_status = 6
    mapping_end_date = 7

class DemandClassification(Enum):
    ERRATIC = 1
    INTERMITTENT = 2
    LUMPY = 3
    SMOOTH = 4

class Variability(Enum):
    X = 1
    Y = 2
    Z = 3

class Segment(Enum):
    A = 1
    B = 2
    C = 3

class EmailTemplate(Enum):
    FORECAST_APPROVAL = 1
    FORECAST_REJECTION = 2
    FORECAST_AUTO_APPROVAL = 3
    FORECAST_APPROVAL_REMINDER = 4

class AlertType(Enum):
    REJECTION = 1
    MODIFIEDBYRM = 2

class ScenarioGroupType(Enum):
    PRODUCT = 1
    LOCATION = 2
    PERSONNEL = 3
    CHANNEL = 4

class CaseStatus(Enum):
    NEW = 1
    FREEZED = 2
    SIMULATIONINPROGRESS = 3
    SIMULATIONSUCCESSFULL = 4
    SIMULATIONFAILED = 5

class AlertConfigurationKeys(Enum):
    alert = 1
    forecast = 2
    isRejectionEmailAlertEnabled = 3
    isApprovalEmailAlertEnabled = 4
    isAutoApprovalEmailAlertEnabled = 5
    isApprovalReminderEmailAlertEnabled = 6
    isOperationalForecastComparisonNotification = 7
    isSalesForecastComparisonNotification = 8
    isUnconstrainedForecastComparisonNotification = 9

class AutoApprovalKeys(Enum):
    autoApproval = 1
    isAutoApprovalEnabled = 2
    autoApprovalReminderDaysBeforeApproval = 3
    approvalDaysOperationalLevel = 4
    approvalDaysSalesLevel = 5
    approvalDaysUnconstrainedLevel = 6

class FileType(Enum):
    PERSONNEL = 1
    PRODUCT = 2
    SALESHISTORYFORECASTMAPPING = 3
    LOCATION = 4
    NETWORK = 5
    FILTER = 6
    PERSONNELMAPPING = 7

class PromotionGroupType(Enum):
    PRODUCT = 1
    LOCATION = 2
    CHANNEL = 3
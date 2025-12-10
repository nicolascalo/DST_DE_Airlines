import datetime

# GCP config
PROJECT_ID = "trusty-anchor-473006-u9"
BUCKET_NAME = "airfrance-bucket"

# Local folder config
DATA_FOLDER = "data"
CALL_PARAMETER_FOLDER = "call_parameter_lists"
API_KEY_FOLDER = "api_keys"

# API limits
MAX_DAILY_API_CALL = 100
MAX_PAGE_TO_FETCH = 10000000000
TIME_DELAY_QUERY = 20  # seconds between API calls
FUTURE_DAYS_TO_RETRIEVE = 365

# Columns that are not part of API call parameters
NON_PARAMETERS = [
    "call_parameters", "response", "message", "timestamp",
    "nb_of_pages_already_retrieved", "totalPages", "completion", "totalFlights"
]

# API URL
BASE_URL = "https://api.airfranceklm.com/opendata/flightstatus/?"

# API skip rules
SKIP_PREVIOUSLY_FAILED_SERVER_ERROR = True
SKIP_PREVIOUSLY_FAILED_FLIGHT_NOT_FOUND = True
SKIP_PREVIOUSLY_FAILED_INVALID_DATE_RANGE = True
SKIP_PREVIOUSLY_FAILED_OTHER_ERRORS = True
SKIP_COMPLETE = True

# Default API headers
DEFAULT_HEADERS = {'Content-Type': 'application/x-www-form-urlencoded'}

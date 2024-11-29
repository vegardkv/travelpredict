# API configuration
ENTUR_API_URL = " https://api.entur.io/journey-planner/v3/graphql"
HEADERS = {
    "ET-Client-Name": "your-client-name"  # Replace with your client name
}
OUTPUT_DIR = "data"

# Default time settings
DEFAULT_START_TIME = "07:00"  # Start time in 24-hour format
DEFAULT_END_TIME = "23:00"    # End time in 24-hour format
DEFAULT_INTERVAL = 300        # Download interval in seconds (e.g., 300 = 5 minutes) 
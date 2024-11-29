import requests
import uuid
import json
from datetime import datetime, timedelta
import schedule
import time
import pathlib
from .config import ENTUR_API_URL, HEADERS, OUTPUT_DIR


def download_realtime_data(query, requestor_id=None):
    """Download real-time data from Entur API using GraphQL
    
    Args:
        requestor_id (str, optional): GUID to identify the requestor. If not provided,
            a random GUID will be generated.
        query (str): GraphQL query string to be executed.
    """
    try:
        # Prepare headers with requestor_id if provided
        request_headers = HEADERS.copy()
        if requestor_id:
            request_headers['ET-Client-Name'] = requestor_id

        # Prepare GraphQL payload
        payload = {'query': query}
            
        response = requests.post(ENTUR_API_URL, 
                               headers=request_headers,
                               json=payload)
                               
        if response.status_code == 200:
            # Generate timestamp for filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"entur_data_{timestamp}.json"

            output = {
                "response": response.json(),
                "timestamp": timestamp
            }
            
            # Save data to file
            with open(pathlib.Path(OUTPUT_DIR, filename), "w", encoding="utf-8") as f:
                json.dump(output, f, ensure_ascii=False)
            
            print(f"Data downloaded successfully at {timestamp}")
        else:
            print(f"Error downloading data: {response.status_code}")
    
    except Exception as e:
        print(f"Error: {str(e)}")


def run_scheduled_downloads(start_time, end_time, interval_seconds, query):
    """Run downloads within specified timeframe
    
    Args:
        start_time (str): Start time in "HH:MM" format
        end_time (str): End time in "HH:MM" format
        interval_seconds (int): Interval between downloads in seconds
        query (str, optional): Query parameter to be passed to the API
    """
    # Convert times to datetime objects
    now = datetime.now()
    start = datetime.strptime(start_time, "%H:%M").replace(
        year=now.year, month=now.month, day=now.day
    )
    end = datetime.strptime(end_time, "%H:%M").replace(
        year=now.year, month=now.month, day=now.day
    )
    
    # If end time is before start time, assume it's for the next day
    if end < start:
        end += timedelta(days=1)
    
    # Schedule the job
    session_id = str(uuid.uuid4())
    schedule.every(interval_seconds).seconds.do(download_realtime_data, query, session_id)
    
    while True:
        current_time = datetime.now()
        
        # Check if we're within the specified timeframe
        if start <= current_time <= end:
            schedule.run_pending()
            time.sleep(1)
        elif current_time > end:
            print("Download period finished")
            break
        else:
            print("Waiting for start time...")
            time.sleep(60)  # Check every minute 

from entur_collector import run_scheduled_downloads
from entur_collector.dataanalysis.convertdata import process_raw_data
import datetime


if __name__ == "__main__":
    current_time = datetime.datetime.now().time()
    if current_time.hour >= 12:
        # This is a hack to make the script run in the afternoon as well.
        # This is convenient for testing purposes.
        START_TIME = "15:00"
        END_TIME = "17:00"
    else:
        START_TIME = "5:00"
        END_TIME = "9:00"

    # Download interval in seconds (e.g., 300 = 5 minutes)
    INTERVAL = 15

    q = open('queries/query1_reduced.txt', 'r').read()
    run_scheduled_downloads(START_TIME, END_TIME, INTERVAL, query=q)
    process_raw_data()
    # download_realtime_data(query=q)

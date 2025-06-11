from entur_collector import run_scheduled_downloads
from entur_collector.dataanalysis.convertdata import process_raw_data
import datetime
import pytz


if __name__ == "__main__":
    current_time = datetime.datetime.now().time()
    now_in_norway = datetime.datetime.now(pytz.timezone("Europe/Oslo"))
    dst = now_in_norway.dst()
    if current_time.hour >= 12:
        # This is a hack to make the script run in the afternoon as well.
        # This is convenient for testing purposes.
        START_TIME = f"{15 - dst.seconds // 3600}:00"
        END_TIME = f"{17 - dst.seconds // 3600}:00"
    else:
        START_TIME = f"{5 - dst.seconds // 3600}:00"
        END_TIME = f"{9 - dst.seconds // 3600}:00"

    # Download interval in seconds (e.g., 300 = 5 minutes)
    INTERVAL = 15

    q = open('queries/query1_reduced.txt', 'r').read()
    run_scheduled_downloads(START_TIME, END_TIME, INTERVAL, query=q)
    process_raw_data()
    # download_realtime_data(query=q)

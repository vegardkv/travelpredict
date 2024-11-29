from entur_collector import run_scheduled_downloads, download_realtime_data


if __name__ == "__main__":
    # Start time in 24-hour format
    START_TIME = "5:00"

    # End time in 24-hour format
    END_TIME = "9:00"

    # Download interval in seconds (e.g., 300 = 5 minutes)
    INTERVAL = 15

    q = open('queries/query1.txt', 'r').read()
    run_scheduled_downloads(START_TIME, END_TIME, INTERVAL, query=q)
    # download_realtime_data(query=q)

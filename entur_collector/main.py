from .collector import run_scheduled_downloads
from .config import DEFAULT_START_TIME, DEFAULT_END_TIME, DEFAULT_INTERVAL

def main():
    run_scheduled_downloads(DEFAULT_START_TIME, DEFAULT_END_TIME, DEFAULT_INTERVAL)

if __name__ == "__main__":
    main() 
from ..config import REFINED_DIR
from .convertdata import read_deviations
from pathlib import Path


def refine_deviations():
    # Create the directory if it does not exist
    refined_dir = Path(REFINED_DIR)
    refined_dir.mkdir(parents=True, exist_ok=True)

    # Load the deviations data
    df = read_deviations()
    df = df[['aimed_arrival', 'timestamp', 'expected_arrival', 'expected_delay']]
    df['day_of_week'] = df['aimed_arrival'].dt.dayofweek
    df['time_of_day'] = df['aimed_arrival'].dt.time
    df['day_since_start'] = (df['aimed_arrival'] - df['aimed_arrival'].min()).dt.days
    df['month'] = df['aimed_arrival'].dt.month
    return df

from ..config import REFINED_DIR
from .convertdata import read_deviations
from pathlib import Path


def refine_deviations():
    # Create the directory if it does not exist
    refined_dir = Path(REFINED_DIR)
    refined_dir.mkdir(parents=True, exist_ok=True)

    # Load the deviations data
    df = read_deviations()
    df = df[[
        'aimed_arrival',
        'timestamp',
        'expected_arrival',
        'expected_delay',
        'day_of_week',
        'time_of_day',
        'month',
        'day_number',
    ]]
    df['day_since_start'] = df['day_number'] - df['day_number'].min()
    return df

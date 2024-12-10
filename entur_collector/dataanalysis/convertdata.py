from entur_collector.models import EnturData
import os
import json
from pathlib import Path
from ..config import RAW_OUTPUT_DIR, PROCESSED_DIR, DEVIATIONS_DIR
import pandas as pd
from datetime import datetime

def parse_data_folder(data_dir='data'):
    """Parse all JSON files in the data directory and return list of EnturData objects
    
    Args:
        data_dir (str): Path to directory containing JSON files
        
    Returns:
        list[EnturData]: List of parsed EnturData objects
    """
    data_files = []
    data_path = Path(data_dir)
    
    # Get all JSON files in directory
    for file in data_path.glob('*.json'):
        with open(file) as f:
            try:
                js = f.read()
                data = EnturData.model_validate_json(js)
                data_files.append(data)
            except Exception as e:
                print(f"Error parsing {file}: {str(e)}")
                
    return data_files


def convert_to_dataframe(data_list: list[EnturData]) -> pd.DataFrame:
    """Convert list of EnturData objects into a flattened pandas DataFrame
    
    Args:
        data_list (list[EnturData]): List of EnturData objects from parse_data_folder
        
    Returns:
        pd.DataFrame: Flattened DataFrame containing all trip data
    """
    flattened_data = []
    
    for data in data_list:
        for trip in data.response.data.stopPlace.estimatedCalls:
            # Create a flat dictionary for each trip
            # Parse the timestamp with a specific time format
            parsed_timestamp = datetime.strptime(data.timestamp, "%Y%m%d_%H%M%S")
            parsed_timestamp = parsed_timestamp.replace(tzinfo=trip.aimedArrivalTime.tzinfo)
            flat_record = {
                'timestamp': parsed_timestamp,
                'realtime': trip.realtime,
                'aimed_arrival': trip.aimedArrivalTime,
                'aimed_departure': trip.aimedDepartureTime,
                'expected_arrival': trip.expectedArrivalTime,
                'expected_departure': trip.expectedDepartureTime,
                'quay_id': trip.quay.id,
                'line_id': trip.serviceJourney.journeyPattern.line.id,
                'line_name': trip.serviceJourney.journeyPattern.line.name,
                'transport_mode': trip.serviceJourney.journeyPattern.line.transportMode
            }
            flattened_data.append(flat_record)
    
    return pd.DataFrame(flattened_data)


def find_deviations():
    df = convert_to_dataframe(parse_data_folder(RAW_OUTPUT_DIR))
    df = df[df.realtime]
    # TODO: remove all entries that has not arrived yet. That, ignore all
    # aimed_arrival/line_id present at the final time stamp, since we do not
    # know when they actually will arrive.
    df['expected_delay'] = df.expected_arrival - df.aimed_arrival
    df['timestamp_delay'] = df.timestamp - df.aimed_arrival
    return df.groupby(['aimed_arrival', 'line_id']).max()


def process_raw_data():
    # Find deviations and save to CSV
    df = find_deviations()
    p = Path(DEVIATIONS_DIR)
    p.mkdir(exist_ok=True)
    timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
    df.to_csv(p / f'deviations_{timestamp}.csv')

    # Move processed data
    p = Path(PROCESSED_DIR)
    p.mkdir(exist_ok=True)
    for file in Path(RAW_OUTPUT_DIR).glob('*.json'):
        os.rename(file, p / file.name)


def save_to_csv(data_dir='data', output_file='trips.csv'):
    """Load JSON files, convert to DataFrame, and save as CSV
    
    Args:
        data_dir (str): Path to directory containing JSON files
        output_file (str): Path where CSV file should be saved
    """
    # Parse JSON files
    data_list = parse_data_folder(data_dir)
    
    if not data_list:
        print("No data files were successfully parsed")
        return
        
    # Convert to DataFrame
    df = convert_to_dataframe(data_list)
    
    # Save to CSV
    df.to_csv(output_file, index=False)
    print(f"Saved {len(df)} records to {output_file}")

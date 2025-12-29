from entur_collector.models import EnturData
import os
import json
from pathlib import Path
from ..config import RAW_OUTPUT_DIR, PROCESSED_DIR, DEVIATIONS_DIR, ROUTE_LINE_ID
from ..database import supabase
import pandas as pd
from datetime import datetime
import tqdm


def parse_data_folder(data_dir=RAW_OUTPUT_DIR):
    """Parse all JSON files in the data directory and return list of EnturData objects
    
    Args:
        data_dir (str): Path to directory containing JSON files
        
    Returns:
        list[EnturData]: List of parsed EnturData objects
    """
    data_path = Path(data_dir)

    def data_files():
        # Get all JSON files in directory
        for file in tqdm.tqdm(data_path.glob('*.json')):
            with open(file) as f:
                try:
                    js = f.read()
                    data = EnturData.model_validate_json(js)
                    yield data
                except Exception as e:
                    print(f"Error parsing {file}: {str(e)}")

    return data_files


def convert_to_dataframe(data_list) -> pd.DataFrame:
    """Convert list of EnturData objects into a flattened pandas DataFrame
    
    Args:
        data_list (list[EnturData]): List of EnturData objects from parse_data_folder
        
    Returns:
        pd.DataFrame: Flattened DataFrame containing all trip data
    """
    flattened_data = []
    
    for data in data_list():
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


def read_deviations(line_id=None):
    """Read deviations from Supabase database.
    
    Args:
        line_id (str, optional): Filter by specific line_id. Defaults to ROUTE_LINE_ID.
        
    Returns:
        pd.DataFrame: DataFrame with deviations and derived columns
    """
    if line_id is None:
        line_id = ROUTE_LINE_ID
    
    # Query Supabase for deviations
    try:
        query = supabase.table('deviations').select('*')
        if line_id:
            query = query.eq('line_id', line_id)
        
        response = query.execute()
        
        if not response.data:
            print(f"No deviations found for line_id={line_id}")
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame(response.data)
        
        # Convert timestamp columns to datetime (no timezone since database uses TIMESTAMP)
        for col in ['aimed_arrival', 'timestamp', 'aimed_departure', 'expected_arrival', 'expected_departure']:
            df[col] = pd.to_datetime(df[col])
        
        # Convert integer seconds back to timedelta
        df['expected_delay'] = pd.to_timedelta(df['expected_delay_seconds'], unit='s')
        df['timestamp_delay'] = pd.to_timedelta(df['timestamp_delay_seconds'], unit='s')
        
        # Add derived columns (matching old _parse_deviations_csv behavior)
        df['day_of_week'] = df['aimed_arrival'].dt.dayofweek
        df['time_of_day'] = df['aimed_arrival'].dt.time
        df['month'] = df['aimed_arrival'].dt.month
        
        # Calculate day_number relative to start_date (naive datetime)
        start_date = pd.Timestamp(year=2024, month=12, day=1)
        df['day_number'] = (df['aimed_arrival'] - start_date).dt.days
        
        return df
        
    except Exception as e:
        print(f"Error reading from database: {e}")
        raise


def process_raw_data():
    """Process raw data and save deviations to Supabase database."""
    # Find deviations
    df = find_deviations()
    
    if len(df) == 0:
        print("No deviations found")
        return
    
    # Reset index to make aimed_arrival and line_id regular columns
    df = df.reset_index()
    
    # Convert timedelta columns to integer seconds for database storage
    df['expected_delay_seconds'] = df['expected_delay'].dt.total_seconds().astype(int)
    df['timestamp_delay_seconds'] = df['timestamp_delay'].dt.total_seconds().astype(int)
    
    # Drop the original timedelta columns (not needed for DB insert)
    df = df.drop(columns=['expected_delay', 'timestamp_delay'])
    
    # Convert datetime columns to naive datetime (remove timezone) and then to ISO format strings
    datetime_columns = ['aimed_arrival', 'timestamp', 'aimed_departure', 'expected_arrival', 'expected_departure']
    for col in datetime_columns:
        if col in df.columns:
            # Remove timezone information by converting to naive datetime
            df[col] = df[col].dt.tz_localize(None).dt.strftime('%Y-%m-%dT%H:%M:%S')
    
    # Convert DataFrame to list of dictionaries for Supabase
    records = df.to_dict('records')
    
    # Batch insert to Supabase (1000 records per batch)
    BATCH_SIZE = 1000
    total_inserted = 0
    
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i+BATCH_SIZE]
        try:
            # Use upsert to handle duplicates (updates existing records with same aimed_arrival+line_id)
            response = supabase.table('deviations').upsert(
                batch,
                on_conflict='aimed_arrival,line_id'
            ).execute()
            total_inserted += len(batch)
            print(f"Inserted/updated batch {i//BATCH_SIZE + 1}: {len(batch)} records")
        except Exception as e:
            print(f"Error inserting batch {i//BATCH_SIZE + 1}: {e}")
            raise
    
    print(f"Total records inserted/updated: {total_inserted}")
    
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

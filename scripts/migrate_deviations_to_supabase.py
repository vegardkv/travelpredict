#!/usr/bin/env python3
"""
Migration script to transfer all deviation CSV files to Supabase database.

This script:
1. Reads all CSV files from the deviations/ folder
2. Deduplicates records keeping the latest timestamp for each (aimed_arrival, line_id) pair
3. Converts timedelta columns to integer seconds
4. Batch uploads to Supabase database
5. Deletes successfully processed CSV files

Usage:
    python scripts/migrate_deviations_to_supabase.py
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from entur_collector.database import supabase
from entur_collector.config import DEVIATIONS_DIR
import pandas as pd
from tqdm import tqdm


def load_all_csv_files(deviations_dir):
    """Load all deviation CSV files and concatenate into a single DataFrame.
    
    Args:
        deviations_dir (str): Path to directory containing CSV files
        
    Returns:
        pd.DataFrame: Combined DataFrame from all CSV files
    """
    csv_files = list(Path(deviations_dir).glob('*.csv'))
    
    if not csv_files:
        print(f"No CSV files found in {deviations_dir}")
        return pd.DataFrame()
    
    print(f"Found {len(csv_files)} CSV files to process")
    
    dfs = []
    for csv_file in tqdm(csv_files, desc="Loading CSV files"):
        try:
            df = pd.read_csv(csv_file)
            # Parse datetime columns
            for col in ['aimed_arrival', 'timestamp', 'aimed_departure', 'expected_arrival', 'expected_departure']:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col])
                    # Remove timezone information (this allows consistent handling later)
                    df[col] = df[col] = df[col].dt.tz_localize(None)
            dfs.append(df)
        except Exception as e:
            print(f"Error loading {csv_file}: {e}")
            continue
    
    if not dfs:
        print("No CSV files were successfully loaded")
        return pd.DataFrame()
    
    # Concatenate all dataframes
    df = pd.concat(dfs, ignore_index=True)
    print(f"Loaded {len(df)} total records from CSV files")
    
    return df


def deduplicate_records(df):
    """Deduplicate records keeping the latest timestamp for each (aimed_arrival, line_id) pair.
    
    Args:
        df (pd.DataFrame): DataFrame with potentially duplicate records
        
    Returns:
        pd.DataFrame: Deduplicated DataFrame
    """
    print(f"Records before deduplication: {len(df)}")
    
    # Group by (aimed_arrival, line_id) and take the record with max timestamp
    df = df.sort_values('timestamp').groupby(['aimed_arrival', 'line_id'], as_index=False).last()
    
    print(f"Records after deduplication: {len(df)}")
    return df


def convert_timedeltas_to_seconds(df):
    """Convert timedelta columns to integer seconds for database storage.
    
    Args:
        df (pd.DataFrame): DataFrame with timedelta columns
        
    Returns:
        pd.DataFrame: DataFrame with timedelta converted to seconds
    """
    # Convert string timedeltas to pandas timedelta if needed
    if 'expected_delay' in df.columns and isinstance(df['expected_delay'].iloc[0], str):
        df['expected_delay'] = pd.to_timedelta(df['expected_delay'])
    if 'timestamp_delay' in df.columns and isinstance(df['timestamp_delay'].iloc[0], str):
        df['timestamp_delay'] = pd.to_timedelta(df['timestamp_delay'])
    
    # Convert to seconds
    df['expected_delay_seconds'] = df['expected_delay'].dt.total_seconds().astype(int)
    df['timestamp_delay_seconds'] = df['timestamp_delay'].dt.total_seconds().astype(int)
    
    # Drop original timedelta columns
    df = df.drop(columns=['expected_delay', 'timestamp_delay'])
    
    return df


def batch_upsert_to_supabase(df, batch_size=1000):
    """Upload records to Supabase in batches.
    
    Args:
        df (pd.DataFrame): DataFrame to upload
        batch_size (int): Number of records per batch
        
    Returns:
        int: Total number of records successfully uploaded
    """
    # Remove any derived columns that aren't in the database schema
    db_columns = [
        'aimed_arrival', 'line_id', 'timestamp', 'realtime',
        'aimed_departure', 'expected_arrival', 'expected_departure',
        'quay_id', 'line_name', 'transport_mode',
        'expected_delay_seconds', 'timestamp_delay_seconds'
    ]
    
    # Keep only columns that exist in both df and db_columns
    existing_columns = [col for col in db_columns if col in df.columns]
    df = df[existing_columns]
    
    # Convert datetime columns to naive datetime (remove timezone) and format as ISO strings
    datetime_columns = ['aimed_arrival', 'timestamp', 'aimed_departure', 'expected_arrival', 'expected_departure']
    for col in datetime_columns:
        if col in df.columns:
            # Format as ISO string without timezone
            df[col] = df[col].apply(lambda x: x.strftime('%Y-%m-%dT%H:%M:%S') if pd.notna(x) else None)
    
    # Convert to list of dictionaries
    records = df.to_dict('records')
    
    total_uploaded = 0
    failed_batches = []
    
    # Upload in batches
    num_batches = (len(records) + batch_size - 1) // batch_size
    
    for i in tqdm(range(0, len(records), batch_size), desc="Uploading to Supabase", total=num_batches):
        batch = records[i:i+batch_size]
        batch_num = i // batch_size + 1
        
        try:
            response = supabase.table('deviations').upsert(
                batch,
                on_conflict='aimed_arrival,line_id'
            ).execute()
            total_uploaded += len(batch)
        except Exception as e:
            print(f"\nError uploading batch {batch_num}: {e}")
            failed_batches.append(batch_num)
            continue
    
    if failed_batches:
        print(f"\n⚠️  Warning: {len(failed_batches)} batches failed to upload: {failed_batches}")
    
    return total_uploaded


def delete_csv_files(deviations_dir):
    """Delete all CSV files in the deviations directory.
    
    Args:
        deviations_dir (str): Path to directory containing CSV files
    """
    csv_files = list(Path(deviations_dir).glob('*.csv'))
    
    if not csv_files:
        return
    
    print(f"\nDeleting {len(csv_files)} CSV files...")
    
    for csv_file in tqdm(csv_files, desc="Deleting CSV files"):
        try:
            csv_file.unlink()
        except Exception as e:
            print(f"Error deleting {csv_file}: {e}")


def main():
    """Main migration process."""
    print("=" * 60)
    print("Deviations CSV to Supabase Migration")
    print("=" * 60)
    
    # Step 1: Load all CSV files
    print("\n[1/5] Loading CSV files...")
    df = load_all_csv_files(DEVIATIONS_DIR)
    
    if df.empty:
        print("No data to migrate. Exiting.")
        return
    
    # Step 2: Deduplicate records
    print("\n[2/5] Deduplicating records...")
    df = deduplicate_records(df)
    
    # Step 3: Convert timedeltas to seconds
    print("\n[3/5] Converting timedeltas to seconds...")
    df = convert_timedeltas_to_seconds(df)
    
    # Step 4: Upload to Supabase
    print(f"\n[4/5] Uploading {len(df)} records to Supabase...")
    total_uploaded = batch_upsert_to_supabase(df)
    
    print(f"\n✅ Successfully uploaded {total_uploaded} records to Supabase")
    
    # Step 5: Delete CSV files
    print("\n[5/5] Cleaning up CSV files...")
    
    response = input("\nDelete all CSV files? (yes/no): ").strip().lower()
    if response == 'yes':
        delete_csv_files(DEVIATIONS_DIR)
        print("✅ CSV files deleted")
    else:
        print("⏭️  Skipped CSV deletion")
    
    print("\n" + "=" * 60)
    print("Migration completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()

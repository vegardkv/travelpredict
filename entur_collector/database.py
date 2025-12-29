"""Database client for Supabase"""
import os
from supabase import create_client, Client
from functools import lru_cache
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


@lru_cache(maxsize=1)
def get_supabase_client() -> Client:
    """Get or create a singleton Supabase client.
    
    Requires environment variables:
    - SUPABASE_URL: Your Supabase project URL
    - SUPABASE_KEY: Your Supabase anon/service key
    
    Returns:
        Client: Supabase client instance
        
    Raises:
        ValueError: If required environment variables are missing
    """
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    
    if not url or not key:
        raise ValueError(
            "Missing Supabase credentials. Set SUPABASE_URL and SUPABASE_KEY "
            "environment variables in .env file or system environment."
        )
    
    return create_client(url, key)


# Convenience alias for easy importing
supabase = get_supabase_client()

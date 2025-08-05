from airflow.hooks.postgres_hook import PostgresHook
import logging
from typing import Optional, Dict, Any
import pandas as pd

# Global variables to cache dimension data
_sources_df = None
_sides_df = None

def _load_sources_dimension(**context) -> pd.DataFrame:
    """
    Load sources dimension table into a pandas DataFrame for fast lookup.
    This function caches the data to avoid repeated database queries.
    
    Returns:
        pd.DataFrame: DataFrame with columns ['id', 'name']
    """
    global _sources_df
    
    if _sources_df is None:
        try:
            # Connect to gold_dw database
            gold_dw_hook = PostgresHook(postgres_conn_id='datawarehouse')
            
            # Query to get all sources
            query = """
            SELECT id, name 
            FROM sources 
            WHERE deleted_at IS NULL 
            ORDER BY id
            """
            
            _sources_df = gold_dw_hook.get_pandas_df(query)
            logging.info(f"Loaded {len(_sources_df)} sources into memory: {list(_sources_df['name'].values)}")
            
        except Exception as e:
            logging.error(f"Error loading sources dimension: {str(e)}")
            raise
    
    return _sources_df

def _load_sides_dimension(**context) -> pd.DataFrame:
    """
    Load sides dimension table into a pandas DataFrame for fast lookup.
    This function caches the data to avoid repeated database queries.
    
    Returns:
        pd.DataFrame: DataFrame with columns ['id', 'name']
    """
    global _sides_df
    
    if _sides_df is None:
        try:
            # Connect to gold_dw database
            gold_dw_hook = PostgresHook(postgres_conn_id='datawarehouse')
            
            # Query to get all sides
            query = """
            SELECT id, name 
            FROM sides 
            WHERE deleted_at IS NULL 
            ORDER BY id
            """
            
            _sides_df = gold_dw_hook.get_pandas_df(query)
            logging.info(f"Loaded {len(_sides_df)} sides into memory: {list(_sides_df['name'].values)}")
            
        except Exception as e:
            logging.error(f"Error loading sides dimension: {str(e)}")
            raise
    
    return _sides_df

def detect_source(source_name: str, **context) -> Optional[int]:
    """
    Detect source ID from sources dimension using in-memory DataFrame lookup.
    This is much more efficient than individual database queries.
    
    Args:
        source_name (str): Name of the source (e.g., 'milli', 'taline', etc.)
        **context: Airflow context
        
    Returns:
        int: Source ID if found, None if not found
        
    Example:
        detect_source('milli') -> 1
        detect_source('taline') -> 2
    """
    try:
        # Load sources dimension (cached)
        sources_df = _load_sources_dimension(**context)
        
        # Find the source ID using pandas lookup
        source_row = sources_df[sources_df['name'] == source_name]
        
        if not source_row.empty:
            source_id = source_row.iloc[0]['id']
            logging.debug(f"Found source '{source_name}' with ID: {source_id}")
            return int(source_id)
        else:
            logging.warning(f"Source '{source_name}' not found in sources table")
            return None
            
    except Exception as e:
        logging.error(f"Error detecting source '{source_name}': {str(e)}")
        raise

def detect_side(side_name: str, **context) -> Optional[int]:
    """
    Detect side ID from sides dimension using in-memory DataFrame lookup.
    This is much more efficient than individual database queries.
    
    Args:
        side_name (str): Name of the side ('buy' or 'sell')
        **context: Airflow context
        
    Returns:
        int: Side ID if found, None if not found
        
    Example:
        detect_side('buy') -> 1
        detect_side('sell') -> 2
    """
    try:
        # Load sides dimension (cached)
        sides_df = _load_sides_dimension(**context)
        
        # Find the side ID using pandas lookup
        side_row = sides_df[sides_df['name'] == side_name]
        
        if not side_row.empty:
            side_id = side_row.iloc[0]['id']
            logging.debug(f"Found side '{side_name}' with ID: {side_id}")
            return int(side_id)
        else:
            logging.warning(f"Side '{side_name}' not found in sides table")
            return None
            
    except Exception as e:
        logging.error(f"Error detecting side '{side_name}': {str(e)}")
        raise

def map_sources_batch(source_names: list, **context) -> Dict[str, int]:
    """
    Map multiple source names to their IDs in batch using in-memory DataFrame.
    This is much more efficient than individual lookups.
    
    Args:
        source_names (list): List of source names to map
        **context: Airflow context
        
    Returns:
        Dict[str, int]: Dictionary mapping source names to their IDs
        
    Example:
        map_sources_batch(['milli', 'taline', 'digikala']) -> {'milli': 1, 'taline': 2, 'digikala': 3}
    """
    try:
        # Load sources dimension (cached)
        sources_df = _load_sources_dimension(**context)
        
        # Create a mapping dictionary
        sources_dict = dict(zip(sources_df['name'], sources_df['id']))
        
        # Map the requested source names
        result = {}
        for source_name in source_names:
            if source_name in sources_dict:
                result[source_name] = int(sources_dict[source_name])
            else:
                logging.warning(f"Source '{source_name}' not found in sources table")
                result[source_name] = None
        
        logging.info(f"Mapped {len(result)} sources: {result}")
        return result
        
    except Exception as e:
        logging.error(f"Error mapping sources batch: {str(e)}")
        raise

def map_sides_batch(side_names: list, **context) -> Dict[str, int]:
    """
    Map multiple side names to their IDs in batch using in-memory DataFrame.
    This is much more efficient than individual lookups.
    
    Args:
        side_names (list): List of side names to map
        **context: Airflow context
        
    Returns:
        Dict[str, int]: Dictionary mapping side names to their IDs
        
    Example:
        map_sides_batch(['buy', 'sell']) -> {'buy': 1, 'sell': 2}
    """
    try:
        # Load sides dimension (cached)
        sides_df = _load_sides_dimension(**context)
        
        # Create a mapping dictionary
        sides_dict = dict(zip(sides_df['name'], sides_df['id']))
        
        # Map the requested side names
        result = {}
        for side_name in side_names:
            if side_name in sides_dict:
                result[side_name] = int(sides_dict[side_name])
            else:
                logging.warning(f"Side '{side_name}' not found in sides table")
                result[side_name] = None
        
        logging.info(f"Mapped {len(result)} sides: {result}")
        return result
        
    except Exception as e:
        logging.error(f"Error mapping sides batch: {str(e)}")
        raise

def get_all_sources(**context) -> Dict[str, int]:
    """
    Get all sources with their IDs from the sources dimension.
    Uses cached DataFrame for efficiency.
    
    Returns:
        Dict[str, int]: Dictionary mapping source names to their IDs
        
    Example:
        get_all_sources() -> {'milli': 1, 'taline': 2, 'digikala': 3, ...}
    """
    try:
        # Load sources dimension (cached)
        sources_df = _load_sources_dimension(**context)
        
        # Create mapping dictionary
        sources_dict = dict(zip(sources_df['name'], sources_df['id']))
        logging.info(f"Retrieved {len(sources_dict)} sources: {list(sources_dict.keys())}")
        
        return sources_dict
        
    except Exception as e:
        logging.error(f"Error getting all sources: {str(e)}")
        raise

def get_all_sides(**context) -> Dict[str, int]:
    """
    Get all sides with their IDs from the sides dimension.
    Uses cached DataFrame for efficiency.
    
    Returns:
        Dict[str, int]: Dictionary mapping side names to their IDs
        
    Example:
        get_all_sides() -> {'buy': 1, 'sell': 2}
    """
    try:
        # Load sides dimension (cached)
        sides_df = _load_sides_dimension(**context)
        
        # Create mapping dictionary
        sides_dict = dict(zip(sides_df['name'], sides_df['id']))
        logging.info(f"Retrieved {len(sides_dict)} sides: {list(sides_dict.keys())}")
        
        return sides_dict
        
    except Exception as e:
        logging.error(f"Error getting all sides: {str(e)}")
        raise

def validate_source_and_side(source_name: str, side_name: str, **context) -> Dict[str, Any]:
    """
    Validate both source and side, returning their IDs if valid.
    Uses optimized in-memory lookups.
    
    Args:
        source_name (str): Name of the source
        side_name (str): Name of the side
        
    Returns:
        Dict[str, Any]: Dictionary with validation results
            {
                'valid': bool,
                'source_id': int or None,
                'side_id': int or None,
                'errors': list of error messages
            }
    """
    errors = []
    source_id = None
    side_id = None
    
    # Validate source
    try:
        source_id = detect_source(source_name, **context)
        if source_id is None:
            errors.append(f"Source '{source_name}' not found")
    except Exception as e:
        errors.append(f"Error validating source '{source_name}': {str(e)}")
    
    # Validate side
    try:
        side_id = detect_side(side_name, **context)
        if side_id is None:
            errors.append(f"Side '{side_name}' not found")
    except Exception as e:
        errors.append(f"Error validating side '{side_name}': {str(e)}")
    
    valid = len(errors) == 0
    
    result = {
        'valid': valid,
        'source_id': source_id,
        'side_id': side_id,
        'errors': errors
    }
    
    if valid:
        logging.info(f"Validation successful: source_id={source_id}, side_id={side_id}")
    else:
        logging.warning(f"Validation failed: {errors}")
    
    return result

def clear_dimension_cache():
    """
    Clear the cached dimension DataFrames.
    Useful for testing or when dimension data has changed.
    """
    global _sources_df, _sides_df
    _sources_df = None
    _sides_df = None
    logging.info("Cleared dimension cache") 
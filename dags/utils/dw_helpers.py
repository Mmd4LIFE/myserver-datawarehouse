from airflow.hooks.postgres_hook import PostgresHook
import logging
from typing import Optional, Dict, Any

def detect_source(source_name: str, **context) -> Optional[int]:
    """
    Detect source ID from sources table in gold_dw database.
    
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
        # Connect to gold_dw database
        gold_dw_hook = PostgresHook(postgres_conn_id='gold_dw')
        
        # Query to get source ID
        query = """
        SELECT id 
        FROM sources 
        WHERE name = %s AND deleted_at IS NULL
        """
        
        result = gold_dw_hook.get_first(query, parameters=(source_name,))
        
        if result:
            source_id = result[0]
            logging.info(f"Found source '{source_name}' with ID: {source_id}")
            return source_id
        else:
            logging.warning(f"Source '{source_name}' not found in sources table")
            return None
            
    except Exception as e:
        logging.error(f"Error detecting source '{source_name}': {str(e)}")
        raise

def detect_side(side_name: str, **context) -> Optional[int]:
    """
    Detect side ID from sides table in gold_dw database.
    
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
        # Connect to gold_dw database
        gold_dw_hook = PostgresHook(postgres_conn_id='gold_dw')
        
        # Query to get side ID
        query = """
        SELECT id 
        FROM sides 
        WHERE name = %s AND deleted_at IS NULL
        """
        
        result = gold_dw_hook.get_first(query, parameters=(side_name,))
        
        if result:
            side_id = result[0]
            logging.info(f"Found side '{side_name}' with ID: {side_id}")
            return side_id
        else:
            logging.warning(f"Side '{side_name}' not found in sides table")
            return None
            
    except Exception as e:
        logging.error(f"Error detecting side '{side_name}': {str(e)}")
        raise

def get_all_sources(**context) -> Dict[str, int]:
    """
    Get all sources with their IDs from the sources table.
    
    Returns:
        Dict[str, int]: Dictionary mapping source names to their IDs
        
    Example:
        get_all_sources() -> {'milli': 1, 'taline': 2, 'digikala': 3, ...}
    """
    try:
        # Connect to gold_dw database
        gold_dw_hook = PostgresHook(postgres_conn_id='gold_dw')
        
        # Query to get all sources
        query = """
        SELECT id, name 
        FROM sources 
        WHERE deleted_at IS NULL 
        ORDER BY id
        """
        
        results = gold_dw_hook.get_records(query)
        
        sources_dict = {row[1]: row[0] for row in results}
        logging.info(f"Retrieved {len(sources_dict)} sources: {list(sources_dict.keys())}")
        
        return sources_dict
        
    except Exception as e:
        logging.error(f"Error getting all sources: {str(e)}")
        raise

def get_all_sides(**context) -> Dict[str, int]:
    """
    Get all sides with their IDs from the sides table.
    
    Returns:
        Dict[str, int]: Dictionary mapping side names to their IDs
        
    Example:
        get_all_sides() -> {'buy': 1, 'sell': 2}
    """
    try:
        # Connect to gold_dw database
        gold_dw_hook = PostgresHook(postgres_conn_id='gold_dw')
        
        # Query to get all sides
        query = """
        SELECT id, name 
        FROM sides 
        WHERE deleted_at IS NULL 
        ORDER BY id
        """
        
        results = gold_dw_hook.get_records(query)
        
        sides_dict = {row[1]: row[0] for row in results}
        logging.info(f"Retrieved {len(sides_dict)} sides: {list(sides_dict.keys())}")
        
        return sides_dict
        
    except Exception as e:
        logging.error(f"Error getting all sides: {str(e)}")
        raise

def validate_source_and_side(source_name: str, side_name: str, **context) -> Dict[str, Any]:
    """
    Validate both source and side, returning their IDs if valid.
    
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
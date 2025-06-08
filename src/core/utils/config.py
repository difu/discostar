import logging
import os
from pathlib import Path
from typing import Any, Dict

import yaml
from dotenv import load_dotenv

from .logging import setup_logging as _setup_logging


def load_config(config_path: str = 'config/settings.yaml') -> Dict[str, Any]:
    """Load configuration from YAML file and environment variables.
    
    Args:
        config_path: Path to the YAML configuration file
        
    Returns:
        Dictionary containing merged configuration
    """
    # Load environment variables from .env file
    load_dotenv()
    
    # Load YAML configuration
    config = {}
    if Path(config_path).exists():
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f) or {}
    
    # Override with environment variables
    config = _merge_env_variables(config)
    
    return config


def _merge_env_variables(config: Dict[str, Any]) -> Dict[str, Any]:
    """Merge environment variables into configuration.
    
    Args:
        config: Base configuration dictionary
        
    Returns:
        Updated configuration with environment variables
    """
    # Discogs configuration
    if 'discogs' not in config:
        config['discogs'] = {}
    if 'api' not in config['discogs']:
        config['discogs']['api'] = {}
    
    # Override Discogs API settings from environment
    if os.getenv('DISCOGS_API_TOKEN'):
        config['discogs']['api']['token'] = os.getenv('DISCOGS_API_TOKEN')
    
    if os.getenv('DISCOGS_USERNAME'):
        config['discogs']['api']['username'] = os.getenv('DISCOGS_USERNAME')
    
    # Database configuration
    if 'database' not in config:
        config['database'] = {}
    
    if os.getenv('DATABASE_URL'):
        config['database']['url'] = os.getenv('DATABASE_URL')
    
    # Azure configuration
    if os.getenv('AZURE_STORAGE_CONNECTION_STRING'):
        if 'azure' not in config:
            config['azure'] = {}
        config['azure']['storage_connection_string'] = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    
    if os.getenv('AZURE_DATABASE_URL'):
        if 'azure' not in config:
            config['azure'] = {}
        config['azure']['database_url'] = os.getenv('AZURE_DATABASE_URL')
    
    # Flask configuration
    if os.getenv('FLASK_SECRET_KEY'):
        if 'flask' not in config:
            config['flask'] = {}
        config['flask']['secret_key'] = os.getenv('FLASK_SECRET_KEY')
    
    if os.getenv('FLASK_ENV'):
        if 'flask' not in config:
            config['flask'] = {}
        config['flask']['env'] = os.getenv('FLASK_ENV')
    
    return config


def setup_logging(level: str = 'INFO') -> None:
    """Setup logging configuration.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    _setup_logging(level)


def get_data_directory() -> Path:
    """Get the data directory path, creating it if necessary.
    
    Returns:
        Path to the data directory
    """
    data_dir = Path('data')
    data_dir.mkdir(exist_ok=True)
    return data_dir


def get_dumps_directory() -> Path:
    """Get the XML dumps directory path, creating it if necessary.
    
    Returns:
        Path to the dumps directory
    """
    dumps_dir = get_data_directory() / 'dumps'
    dumps_dir.mkdir(exist_ok=True)
    return dumps_dir


def get_cache_directory() -> Path:
    """Get the cache directory path, creating it if necessary.
    
    Returns:
        Path to the cache directory
    """
    cache_dir = get_data_directory() / 'cache'
    cache_dir.mkdir(exist_ok=True)
    return cache_dir


def validate_config(config: Dict[str, Any]) -> bool:
    """Validate that required configuration is present.
    
    Args:
        config: Configuration dictionary to validate
        
    Returns:
        True if configuration is valid, False otherwise
    """
    required_for_api = [
        ('discogs', 'api', 'token'),
        ('discogs', 'api', 'username')
    ]
    
    missing = []
    for path in required_for_api:
        current = config
        for key in path:
            if key not in current:
                missing.append('.'.join(path))
                break
            current = current[key]
    
    if missing:
        logging.error(f"Missing required configuration: {', '.join(missing)}")
        logging.error("Please set DISCOGS_API_TOKEN and DISCOGS_USERNAME environment variables")
        return False
    
    return True
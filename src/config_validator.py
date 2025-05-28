"""
Configuration validation module.
"""
import logging
from typing import Dict, Any
import yaml

logger = logging.getLogger(__name__)

class ConfigValidator:
    """Validates reconciliation configuration."""
    
    @staticmethod
    def load_and_validate(config_path: str) -> Dict[str, Any]:
        """
        Load and validate configuration from YAML file.
        
        Args:
            config_path: Path to YAML configuration file
            
        Returns:
            Validated configuration dictionary
        """
        config = ConfigValidator._load_yaml(config_path)
        ConfigValidator._validate_config(config)
        return config
    
    @staticmethod
    def _load_yaml(path: str) -> Dict[str, Any]:
        """Load YAML file."""
        try:
            with open(path, 'r') as file:
                return yaml.safe_load(file)
        except Exception as e:
            logger.error(f"Failed to load config file {path}: {str(e)}")
            raise
    
    @staticmethod
    def _validate_config(config: Dict[str, Any]):
        """Validate configuration structure and required fields."""
        # Validate sources
        if 'sources' not in config:
            raise ValueError("Configuration must include 'sources' section")
            
        for source_type in ['internal', 'external']:
            if source_type not in config['sources']:
                raise ValueError(f"Configuration must include '{source_type}' source")
            
            source = config['sources'][source_type]
            if 'type' not in source:
                raise ValueError(f"Source '{source_type}' must specify 'type'")
                
            if source['type'] == 'file':
                if 'path' not in source:
                    raise ValueError(f"File source '{source_type}' must specify 'path'")
            else:
                if 'query' not in source:
                    raise ValueError(f"Database source '{source_type}' must specify 'query'")
                if 'connection' not in source:
                    raise ValueError(f"Database source '{source_type}' must specify 'connection' parameters")
        
        # Validate matching configuration
        if 'matching' not in config:
            raise ValueError("Configuration must include 'matching' section")
            
        matching = config['matching']
        required_matching_fields = ['match_on', 'internal_key', 'external_key']
        for field in required_matching_fields:
            if field not in matching:
                raise ValueError(f"Matching configuration must include '{field}'")
        
        # Validate output configuration
        if 'output' not in config:
            raise ValueError("Configuration must include 'output' section")
            
        output = config['output']
        required_output_fields = ['path', 'reconciled_file', 'unmatched_internal_file', 'unmatched_external_file']
        for field in required_output_fields:
            if field not in output:
                raise ValueError(f"Output configuration must include '{field}'")
        
        logger.info("Configuration validation successful") 
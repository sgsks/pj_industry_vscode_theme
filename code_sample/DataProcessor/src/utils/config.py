import os
from typing import Dict, Any
from dataclasses import dataclass
from dotenv import load_dotenv

@dataclass
class ProcessingConfig:
    """Configuration settings for data processing."""
    
    # Processing settings
    batch_size: int = 1000
    max_workers: int = 4
    missing_value_strategy: str = 'mean'
    
    # Input validation
    validate_schema: bool = True
    allow_unknown_columns: bool = False
    
    # Performance settings
    cache_enabled: bool = True
    cache_size: int = 1000
    
    # Logging settings
    log_level: str = 'INFO'
    log_file: str = 'processing.log'

    @classmethod
    def from_env(cls) -> 'ProcessingConfig':
        """Create configuration from environment variables."""
        load_dotenv()
        
        return cls(
            batch_size=int(os.getenv('DP_BATCH_SIZE', 1000)),
            max_workers=int(os.getenv('DP_MAX_WORKERS', 4)),
            missing_value_strategy=os.getenv('DP_MISSING_STRATEGY', 'mean'),
            validate_schema=bool(os.getenv('DP_VALIDATE_SCHEMA', True)),
            allow_unknown_columns=bool(os.getenv('DP_ALLOW_UNKNOWN', False)),
            cache_enabled=bool(os.getenv('DP_CACHE_ENABLED', True)),
            cache_size=int(os.getenv('DP_CACHE_SIZE', 1000)),
            log_level=os.getenv('DP_LOG_LEVEL', 'INFO'),
            log_file=os.getenv('DP_LOG_FILE', 'processing.log')
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            'processing': {
                'batch_size': self.batch_size,
                'max_workers': self.max_workers,
                'missing_value_strategy': self.missing_value_strategy
            },
            'validation': {
                'validate_schema': self.validate_schema,
                'allow_unknown_columns': self.allow_unknown_columns
            },
            'performance': {
                'cache_enabled': self.cache_enabled,
                'cache_size': self.cache_size
            },
            'logging': {
                'level': self.log_level,
                'file': self.log_file
            }
        }

DEFAULT_CONFIG = ProcessingConfig()
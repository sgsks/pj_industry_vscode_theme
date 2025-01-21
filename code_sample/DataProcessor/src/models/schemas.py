from dataclasses import dataclass, field
from typing import Optional, Dict, Any
from datetime import datetime
import pandas as pd

@dataclass
class DatasetSchema:
    """Schema for input dataset validation."""
    
    name: str
    version: str
    created_at: datetime = field(default_factory=datetime.now)
    required_columns: list[str] = field(default_factory=list)
    column_types: Dict[str, str] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def validate_dataframe(self, df: pd.DataFrame) -> bool:
        """
        Validate if the DataFrame matches the schema.
        
        Args:
            df (pd.DataFrame): DataFrame to validate
            
        Returns:
            bool: True if valid, raises ValueError if invalid
        """
        # Check required columns
        missing_columns = set(self.required_columns) - set(df.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        # Check column types
        for col, expected_type in self.column_types.items():
            if col in df.columns:
                actual_type = df[col].dtype
                if str(actual_type) != expected_type:
                    raise ValueError(
                        f"Column '{col}' has type {actual_type}, "
                        f"expected {expected_type}"
                    )

        return True

    def __str__(self) -> str:
        return (f"DatasetSchema(name={self.name}, version={self.version}, "
                f"columns={len(self.required_columns)})")

@dataclass
class ProcessingResult:
    """Container for processing results."""
    
    data: Optional[pd.DataFrame]
    stats: Dict[str, Any]
    success: bool
    processed_at: datetime = field(default_factory=datetime.now)

    @property
    def summary(self) -> Dict[str, Any]:
        """Generate a summary of the processing results."""
        return {
            'success': self.success,
            'record_count': len(self.data) if self.data is not None else 0,
            'processing_time': self.stats.get('processing_time', 0),
            'processed_at': self.processed_at.isoformat()
        }

class ProcessingError(Exception):
    """Custom exception for processing errors."""
    
    def __init__(self, message: str, details: Optional[Dict] = None):
        super().__init__(message)
        self.details = details or {}
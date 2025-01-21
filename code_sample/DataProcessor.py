# Python Example - Data Processing Pipeline
from typing import List, Dict, Optional
import pandas as pd
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class DataProcessor:
    """
    Processes raw data from multiple sources and prepares it for analysis.
    Handles data cleaning, transformation, and validation.
    """
    
    def __init__(self, config: Dict[str, any]):
        self.config = config
        self.transformation_rules = self._load_rules()
        logger.info("DataProcessor initialized with %d rules", len(self.transformation_rules))

    async def process_batch(self, data: pd.DataFrame) -> Optional[pd.DataFrame]:
        try:
            if data.empty:
                logger.warning("Received empty dataset")
                return None

            # Apply data cleaning rules
            cleaned_data = self._remove_duplicates(data)
            cleaned_data = self._handle_missing_values(cleaned_data)

            # Apply transformations
            result = cleaned_data.copy()
            for rule in self.transformation_rules:
                result = rule.apply(result)
                logger.debug(f"Applied transformation: {rule.name}")

            self._validate_output(result)
            return result

        except Exception as e:
            logger.error("Error processing batch: %s", str(e))
            raise ProcessingError(f"Batch processing failed: {str(e)}")

    def _validate_output(self, df: pd.DataFrame) -> bool:
        """Validates the processed data against defined rules."""
        required_columns = self.config["required_columns"]
        missing_columns = set(required_columns) - set(df.columns)
        
        if missing_columns:
            raise ValidationError(f"Missing required columns: {missing_columns}")
        return True

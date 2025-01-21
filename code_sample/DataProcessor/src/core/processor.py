from typing import Dict, List, Optional
import pandas as pd
from datetime import datetime
from ..models.schemas import DatasetSchema, ProcessingResult
from ..utils.logger import setup_logger

logger = setup_logger(__name__)

class DataProcessor:
    """Main class for data processing operations."""

    DEFAULT_BATCH_SIZE = 1000
    SUPPORTED_FORMATS = {'csv', 'parquet', 'json'}

    def __init__(self, config: Dict[str, any]) -> None:
        self.config = config
        self._processed_count = 0
        self._start_time = datetime.now()

    async def process_dataset(self, data: pd.DataFrame) -> ProcessingResult:
        """
        Process the input dataset according to configured rules.

        Args:
            data (pd.DataFrame): Input data to process

        Returns:
            ProcessingResult: Results of the processing operation
        """
        try:
            # Input validation
            if data.empty:
                raise ValueError("Empty dataset provided")

            # Apply transformations
            processed_data = self._preprocess_data(data)
            processed_data = self._remove_outliers(processed_data)
            processed_data = self._handle_missing_values(processed_data)

            # Calculate statistics
            stats = {
                'record_count': len(processed_data),
                'missing_values': processed_data.isnull().sum().to_dict(),
                'processing_time': (datetime.now() - self._start_time).seconds
            }

            logger.info(f"Successfully processed {stats['record_count']} records")
            self._processed_count += stats['record_count']

            return ProcessingResult(
                data=processed_data,
                stats=stats,
                success=True
            )

        except Exception as e:
            logger.error(f"Processing failed: {str(e)}")
            return ProcessingResult(
                data=None,
                stats={'error': str(e)},
                success=False
            )

    def _preprocess_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply preprocessing steps to the data."""
        result = df.copy()
        
        # Remove duplicates
        initial_count = len(result)
        result = result.drop_duplicates()
        if len(result) != initial_count:
            logger.info(f"Removed {initial_count - len(result)} duplicate records")
            
        return result

    def _remove_outliers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove statistical outliers using IQR method."""
        numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
        result = df.copy()
        
        for col in numeric_cols:
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            outlier_mask = ~((df[col] < (Q1 - 1.5 * IQR)) | 
                           (df[col] > (Q3 + 1.5 * IQR)))
            result = result[outlier_mask]
            
        return result

    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle missing values according to configuration."""
        strategy = self.config.get('missing_value_strategy', 'mean')
        result = df.copy()

        if strategy == 'drop':
            result = result.dropna()
        elif strategy == 'mean':
            result = result.fillna(df.mean())
        elif strategy == 'median':
            result = result.fillna(df.median())
        else:
            logger.warning(f"Unknown strategy '{strategy}', falling back to mean")
            result = result.fillna(df.mean())

        return result

    @property
    def processing_stats(self) -> Dict[str, any]:
        """Return current processing statistics."""
        return {
            'total_processed': self._processed_count,
            'uptime_seconds': (datetime.now() - self._start_time).seconds,
            'average_throughput': (
                self._processed_count / 
                (datetime.now() - self._start_time).seconds 
                if (datetime.now() - self._start_time).seconds > 0 else 0
            )
        }
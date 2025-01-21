import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from src.core.processor import DataProcessor
from src.models.schemas import DatasetSchema, ProcessingError
from src.utils.config import ProcessingConfig

@pytest.fixture
def sample_data() -> pd.DataFrame:
    """Create sample data for testing."""
    return pd.DataFrame({
        'id': range(1, 101),
        'value': np.random.normal(50, 10, 100),
        'category': np.random.choice(['A', 'B', 'C'], 100)
    })

@pytest.fixture
def processor() -> DataProcessor:
    """Create a configured processor instance."""
    config = ProcessingConfig(
        batch_size=100,
        missing_value_strategy='mean'
    )
    return DataProcessor(config)

@pytest.fixture
def schema() -> DatasetSchema:
    """Create a test schema."""
    return DatasetSchema(
        name='test_dataset',
        version='1.0',
        required_columns=['id', 'value'],
        column_types={
            'id': 'int64',
            'value': 'float64',
            'category': 'object'
        }
    )

class TestDataProcessor:
    """Test suite for DataProcessor functionality."""
    
    @pytest.mark.asyncio
    async def test_process_dataset_with_valid_data(self, 
                                                 processor: DataProcessor,
                                                 sample_data: pd.DataFrame):
        """Test processing with valid input data."""
        # When
        result = await processor.process_dataset(sample_data)
        
        # Then
        assert result.success
        assert result.data is not None
        assert len(result.data) > 0
        assert 'record_count' in result.stats
        assert result.stats['record_count'] == len(result.data)

    @pytest.mark.asyncio
    async def test_process_dataset_with_missing_values(self, 
                                                     processor: DataProcessor,
                                                     sample_data: pd.DataFrame):
        """Test processing with missing values."""
        # Given
        sample_data.loc[0:10, 'value'] = np.nan
        
        # When
        result = await processor.process_dataset(sample_data)
        
        # Then
        assert result.success
        assert result.data is not None
        assert not result.data['value'].isnull().any()

    @pytest.mark.asyncio
    async def test_process_dataset_with_outliers(self, 
                                               processor: DataProcessor,
                                               sample_data: pd.DataFrame):
        """Test outlier removal."""
        # Given
        sample_data.loc[0, 'value'] = 1000  # Add an outlier
        
        # When
        result = await processor.process_dataset(sample_data)
        
        # Then
        assert result.success
        assert result.data is not None
        assert len(result.data) < len(sample_data)
        assert result.data['value'].max() < 1000

    def test_processing_stats(self, processor: DataProcessor):
        """Test processing statistics tracking."""
        # When
        stats = processor.processing_stats
        
        # Then
        assert 'total_processed' in stats
        assert 'uptime_seconds' in stats
        assert 'average_throughput' in stats
        assert isinstance(stats['average_throughput'], float)

    @pytest.mark.asyncio
    async def test_empty_dataset(self, processor: DataProcessor):
        """Test processing with empty dataset."""
        # Given
        empty_data = pd.DataFrame()
        
        # When
        result = await processor.process_dataset(empty_data)
        
        # Then
        assert not result.success
        assert result.data is None
        assert 'error' in result.stats
        assert 'Empty dataset' in result.stats['error']

    @pytest.mark.asyncio
    async def test_schema_validation(self, 
                                   processor: DataProcessor, 
                                   sample_data: pd.DataFrame,
                                   schema: DatasetSchema):
        """Test dataset schema validation."""
        # Given
        sample_data = sample_data.drop('category', axis=1)  # Remove optional column
        
        # When
        try:
            schema.validate_dataframe(sample_data)
            valid = True
        except ValueError:
            valid = False
            
        # Then
        assert valid
        assert set(schema.required_columns).issubset(set(sample_data.columns))

    @pytest.mark.asyncio
    async def test_invalid_schema(self, 
                                processor: DataProcessor, 
                                sample_data: pd.DataFrame,
                                schema: DatasetSchema):
        """Test validation with invalid schema."""
        # Given
        sample_data = sample_data.drop('id', axis=1)  # Remove required column
        
        # When/Then
        with pytest.raises(ValueError) as error:
            schema.validate_dataframe(sample_data)
        assert "Missing required columns" in str(error.value)

    @pytest.mark.asyncio
    async def test_type_validation(self, 
                                 processor: DataProcessor, 
                                 sample_data: pd.DataFrame,
                                 schema: DatasetSchema):
        """Test column type validation."""
        # Given
        sample_data['value'] = sample_data['value'].astype(str)  # Wrong type
        
        # When/Then
        with pytest.raises(ValueError) as error:
            schema.validate_dataframe(sample_data)
        assert "expected float64" in str(error.value)

    @pytest.mark.asyncio
    async def test_performance_metrics(self, 
                                    processor: DataProcessor,
                                    sample_data: pd.DataFrame):
        """Test performance metrics calculation."""
        # When
        initial_stats = processor.processing_stats
        result = await processor.process_dataset(sample_data)
        final_stats = processor.processing_stats
        
        # Then
        assert result.success
        assert final_stats['total_processed'] > initial_stats['total_processed']
        assert final_stats['uptime_seconds'] >= initial_stats['uptime_seconds']
        assert 'average_throughput' in final_stats

    @pytest.mark.asyncio
    async def test_duplicate_handling(self, 
                                    processor: DataProcessor,
                                    sample_data: pd.DataFrame):
        """Test duplicate record handling."""
        # Given
        duplicated_data = pd.concat([sample_data, sample_data.head(10)])
        
        # When
        result = await processor.process_dataset(duplicated_data)
        
        # Then
        assert result.success
        assert len(result.data) < len(duplicated_data)
        assert len(result.data) == len(result.data.drop_duplicates())

    @pytest.mark.asyncio
    async def test_missing_value_strategies(self):
        """Test different missing value handling strategies."""
        # Given
        data = pd.DataFrame({
            'id': range(1, 6),
            'value': [1.0, np.nan, 3.0, np.nan, 5.0]
        })
        
        strategies = ['mean', 'median', 'drop']
        results = {}
        
        # When
        for strategy in strategies:
            config = ProcessingConfig(missing_value_strategy=strategy)
            processor = DataProcessor(config)
            result = await processor.process_dataset(data)
            results[strategy] = result
            
        # Then
        assert all(r.success for r in results.values())
        assert len(results['drop'].data) < len(data)  # Should have fewer rows
        assert not results['mean'].data['value'].isnull().any()  # No missing values
        assert not results['median'].data['value'].isnull().any()

if __name__ == '__main__':
    pytest.main([__file__])
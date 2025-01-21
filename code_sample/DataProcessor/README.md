# Data Processor

A Python library for processing large datasets with support for validation, transformation, and analysis operations.

## Features

- Batch processing of large datasets
- Configurable data validation
- Multiple strategies for handling missing values
- Outlier detection and removal
- Performance monitoring and statistics
- Comprehensive logging system

## Installation

```bash
pip install -e .
```

For development:
```bash
pip install -e ".[dev]"
```

## Usage

Basic usage example:

```python
from data_processor.core.processor import DataProcessor
from data_processor.utils.config import ProcessingConfig

# Configure the processor
config = ProcessingConfig(
    batch_size=1000,
    missing_value_strategy='mean'
)

# Create processor instance
processor = DataProcessor(config)

# Process dataset
result = await processor.process_dataset(your_dataframe)

# Access results
if result.success:
    processed_data = result.data
    stats = result.stats
    print(f"Processed {stats['record_count']} records")
```

## Testing

Run tests using pytest:

```bash
pytest tests/
```

With coverage:
```bash
pytest --cov=src tests/
```

## Configuration

Configuration can be set using environment variables:

- `DP_BATCH_SIZE`: Size of processing batches
- `DP_MAX_WORKERS`: Number of worker threads
- `DP_MISSING_STRATEGY`: Strategy for handling missing values
- `DP_LOG_LEVEL`: Logging level
- `DP_LOG_FILE`: Log file path

See `.env.example` for all available options.

## License

MIT
import logging
import sys
from typing import Optional
from datetime import datetime
from pathlib import Path

def setup_logger(
    name: str,
    log_level: str = 'INFO',
    log_file: Optional[str] = None
) -> logging.Logger:
    """
    Configure and return a logger instance.
    
    Args:
        name (str): Logger name
        log_level (str): Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file (Optional[str]): Path to log file, if file logging is desired
        
    Returns:
        logging.Logger: Configured logger instance
    """
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, log_level.upper()))

    # Create formatters
    detailed_formatter = logging.Formatter(
        '%(asctime)s | %(name)s | %(levelname)s | %(message)s'
    )
    simple_formatter = logging.Formatter(
        '%(levelname)s | %(message)s'
    )

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(simple_formatter)
    logger.addHandler(console_handler)

    # File handler (if requested)
    if log_file:
        # Ensure log directory exists
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Create handler with rotation
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(detailed_formatter)
        logger.addHandler(file_handler)

    return logger

class DataProcessingLogger:
    """Custom logger for data processing operations."""
    
    def __init__(self, name: str, log_file: Optional[str] = None):
        self.logger = setup_logger(name, log_file=log_file)
        self.start_time = datetime.now()
        self.processing_count = 0

    def log_operation(self, 
                     operation: str, 
                     status: str, 
                     details: Optional[dict] = None):
        """Log a processing operation with timing information."""
        duration = (datetime.now() - self.start_time).total_seconds()
        
        message = (
            f"Operation: {operation} | "
            f"Status: {status} | "
            f"Duration: {duration:.2f}s"
        )
        
        if details:
            message += f" | Details: {details}"
        
        if status == 'ERROR':
            self.logger.error(message)
        elif status == 'WARNING':
            self.logger.warning(message)
        else:
            self.logger.info(message)

    def log_progress(self, current: int, total: int):
        """Log processing progress."""
        percentage = (current / total) * 100 if total > 0 else 0
        self.logger.info(f"Progress: {current}/{total} ({percentage:.1f}%)")

    def log_metrics(self, metrics: dict):
        """Log processing metrics."""
        self.logger.info("Processing Metrics:")
        for key, value in metrics.items():
            self.logger.info(f"  {key}: {value}")
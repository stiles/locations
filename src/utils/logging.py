import logging
import os
from datetime import datetime


def setup_logging(log_level: str = "INFO", log_file: str = None):
    """Configure centralized logging for the location scraping system"""
    
    # Create logs directory if it doesn't exist
    if log_file:
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
    
    # Configure logging format
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    # Set up root logger
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format=log_format,
        handlers=[
            logging.StreamHandler(),  # Console output
            *([logging.FileHandler(log_file)] if log_file else [])  # File output if specified
        ]
    )
    
    # Configure specific loggers
    loggers = [
        'src.core.base_scraper',
        'src.core.data_processor', 
        'src.core.storage',
        'src.scrapers',
        'src.utils'
    ]
    
    for logger_name in loggers:
        logger = logging.getLogger(logger_name)
        logger.setLevel(getattr(logging, log_level.upper()))
    
    # Suppress verbose third-party loggers
    logging.getLogger('boto3').setLevel(logging.WARNING)
    logging.getLogger('botocore').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    
    root_logger = logging.getLogger()
    root_logger.info(f"Logging initialized at {log_level} level")
    
    return root_logger 
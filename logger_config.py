import logging
import os
import json
from logging.handlers import RotatingFileHandler

class DetailFormatter(logging.Formatter):
    def format(self, record):
        log_string = super().format(record)
        extra_items = {k:v for k,v in record.__dict__.items() if k not in logging.LogRecord.__dict__ and k != 'args'}
        if extra_items:
            details_str = json.dumps(extra_items)
            log_string += f" -- Details: {details_str}"
        return log_string

def setup_logger(config):
    log_level = config.get('logging', 'log_level', fallback='INFO').upper()
    log_file = config.get('logging', 'log_file_path', fallback='logs/scraper.log')
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    logger = logging.getLogger("slapdotred_scraper")
    logger.setLevel(log_level)
    
    if logger.hasHandlers():
        logger.handlers.clear()

    fh = RotatingFileHandler(log_file, maxBytes=1*1024*1024, backupCount=5)
    formatter = DetailFormatter('%(asctime)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger
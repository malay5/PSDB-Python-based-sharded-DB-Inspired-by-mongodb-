# request_logger.py
import logging
from datetime import datetime
import os

_loggers = {}

def get_request_logger(worker_port: int):
    """
    Returns a logger instance specific to the given worker port.
    """
    if worker_port in _loggers:
        return _loggers[worker_port]

    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"worker_{worker_port}_request_log.txt")

    logger = logging.getLogger(f"RequestLogger-{worker_port}")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        file_handler = logging.FileHandler(log_file)
        formatter = logging.Formatter('%(asctime)s - %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    _loggers[worker_port] = logger
    return logger

def log_request(worker_port: int, message: str):
    logger = get_request_logger(worker_port)
    logger.info(message)

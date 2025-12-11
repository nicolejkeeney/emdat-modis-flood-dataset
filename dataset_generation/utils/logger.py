"""
logger.py

"""

import logging
import os
from datetime import datetime


def setup_logger(log_dir, log_filename, timestamp, verbose=True):
    """
    Initialize a timestamped logger.

    Parameters
    ----------
    log_dir : str
        Directory to save logs. Defaults to "qaqc_logs".
    log_filename: str
        Name to give logger. Timestamp will be appended
    timestamp: str
        Timestamp
    verbose : bool
        If True, also logs to console.

    Returns
    -------
    logger : logging.Logger
        Configured logger instance.
    log_filepath : str
        Full path to the created log file.
    """
    log_filename = f"{log_filename}_{timestamp}.log"
    log_filepath = f"{log_dir}/{log_filename}"

    logger = _init_logger(log_filename, logs_dir=log_dir, verbose=verbose)
    logger.info(f"Logger initialized: {log_filepath}")

    return logger, log_filepath


def close_logger(logger):
    """
    Closes all handlers associated with the given logger.

    Parameters
    ----------
    logger : logging.Logger
        The logger instance to close.
    """
    for handler in logger.handlers[:]:  # Copy the list to avoid modification issues
        handler.close()
        logger.removeHandler(handler)


def _configure_logger(log_file: str, verbose: bool) -> logging.Logger:
    """
    Configures a logger to write to a file, with optional console output.

    Parameters
    ----------
    log_file : str
        Full path to the log file. Will overwrite existing file if present.
    verbose : bool
        If True, logs are also printed to the console.

    Returns
    -------
    logging.Logger
        Configured logger instance.
    """
    # Create or retrieve a logger instance named "sharedLogger"
    logger = logging.getLogger("sharedLogger")

    # If the logger is already configured with handlers, return it as-is
    if logger.hasHandlers():
        return logger

    # Set the logger to capture all messages at DEBUG level and above
    logger.setLevel(logging.DEBUG)

    # Create a file handler in overwrite mode
    file_handler = logging.FileHandler(log_file, mode="w")  # Overwrite if file exists
    file_handler.setLevel(logging.DEBUG)

    # Define log message format
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)

    # Add the file handler to the logger
    logger.addHandler(file_handler)

    # Optionally, also add console logging if verbose is True
    if verbose:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger


def _init_logger(log_fname, logs_dir, verbose=True) -> logging.Logger:
    """
    Initializes a timestamped logger with optional console output.

    Parameters
    ----------
    log_fname : str
        Log filename .
    logs_dir : str, optional
        Directory to store log files.
    verbose : bool, optional
        If True, also logs to the console. Defaults to True.

    Returns
    -------
    logging.Logger
        Configured logger instance.
    """

    # Create the logs directory if it does not exist
    if not os.path.isdir(logs_dir):
        os.makedirs(logs_dir, exist_ok=True)
        print(f"Created directory: {logs_dir}")

    # Construct the full path to the log file
    log_path = os.path.join(logs_dir, log_fname)

    # Configure and return the logger
    return _configure_logger(log_path, verbose=verbose)

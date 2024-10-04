"""Logging setup."""

import logging
import logging.config
import json
import os


LOG_DIR = "log"
LOGGER_NAME = "kimchi"


def get_log_filename(source_file: str) -> str:
    file_name = os.path.basename(source_file)
    base_name, extension = os.path.splitext(file_name)
    log_name = os.path.join(LOG_DIR, f"{base_name}.log")
    return log_name


def get_logger() -> logging.Logger:
    logger = logging.getLogger(LOGGER_NAME)
    return logger


def get_script_logger(source_file: str | None = None) -> logging.Logger:
    with open("logging_config.json", "r") as fp:
        dict_config = json.load(fp)
    if source_file is not None:
        log_filename = get_log_filename(source_file)
        handler_name = "info_rotating_file_handler"
        dict_config["handlers"][handler_name] = {
            "level": "INFO",
            "formatter": "standard",
            "class": "logging.handlers.RotatingFileHandler",
            "filename": log_filename,
            "mode": "a",
            "maxBytes": 1_048_576,
            "backupCount": 10,
        }
        dict_config["loggers"][LOGGER_NAME]["handlers"].append(handler_name)
    logging.config.dictConfig(dict_config)
    logger = logging.getLogger(LOGGER_NAME)
    return logger

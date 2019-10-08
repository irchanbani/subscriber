import logging
import logging.config

import os


def setup_logging(name, handler_name="basic", log_level="INFO", **kwargs):
    """
    Setup Python logging.

    Args:
        name (str): Name, will be log filename for handler rotating_file and
            the name for handler stackdriver
        handler_name (str): Log handler. Must be one of:
            - basic
            - rotating_file
            - stackdriver
        log_level (str): Python's logging level
        **kwargs:
            log_dir(str) for log handler "rotating_file"

    """
    if handler_name is not None and not isinstance(handler_name, str):
        raise ValueError("handler_name should be an instance of str")

    supported_handler_names = {"basic", "rotating_file", "stackdriver"}
    if handler_name not in supported_handler_names:
        raise ValueError(
            "handler_name must be one of {}".format(supported_handler_names)
        )

    log_format = (
        '{"time": "%(asctime)s", '
        '"log_level": "%(levelname)s", '
        '"PID": %(process)d, '
        '"process_name": "%(processName)s", '
        '"thread": %(thread)d, '
        '"thread_name": "%(threadName)s", '
        '"name": "%(name)s", '
        '"message": "%(message)s"}'
    )
    log_format = kwargs.get("log_format", log_format)

    if handler_name == "basic":
        logging.basicConfig(level=log_level, format=log_format)
    elif handler_name == "rotating_file":
        log_dir = kwargs.get("log_dir", "")
        if log_dir:
            filename = os.path.join(log_dir, name + ".log")
            setup_rotating_file(log_level, filename, log_format)
        else:
            raise ValueError(
                "log_dir must be specified when using rotating_file"
            )
    elif handler_name == "stackdriver":
        if "excluded_loggers" in kwargs:
            excluded_loggers = kwargs["excluded_loggers"]
        else:
            excluded_loggers = None

        setup_stackdriver(log_level, name=name, log_format=log_format,
                          excluded_loggers=excluded_loggers)


def setup_rotating_file(log_level, filename, log_format):
    configured_log_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {
                "format": log_format
            },
        },
        "handlers": {
            "default": {
                "level": "DEBUG",
                "class": "logging.handlers.TimedRotatingFileHandler",
                "filename": filename,
                "when": "midnight",
                "formatter": "standard",
                "encoding": "utf-8"
            },
        },
        "loggers": {
            "": {
                "handlers": ["default"],
                "level": log_level,
                "propagate": True
            }
        }
    }

    logging.config.dictConfig(configured_log_config)


# the package stated in requirements is google-cloud-logging, not `google`
# noinspection PyPackageRequirements
def setup_stackdriver(log_level, name, log_format,
                      excluded_loggers=None):
    try:
        from google.cloud.logging import Client
        from google.cloud.logging import handlers as google_logging_handlers
        from google.cloud.logging.handlers.handlers import \
            EXCLUDED_LOGGER_DEFAULTS, \
            CloudLoggingHandler
    except ImportError:
        raise ValueError(
            "google-cloud-logging is not properly installed"
        )

    if not excluded_loggers:
        excluded_loggers = EXCLUDED_LOGGER_DEFAULTS
    client = Client()

    # the docstring of CloudLoggingHandler point to client instead of Client
    # noinspection PyTypeChecker
    handler = CloudLoggingHandler(client, name)
    handler.setFormatter(logging.Formatter(log_format, None, "%"))

    google_logging_handlers.setup_logging(
        handler, log_level=log_level, excluded_loggers=excluded_loggers
    )

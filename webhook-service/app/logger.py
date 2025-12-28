# import structlog

# def configure_logging():
#     structlog.configure(
#         processors=[
#             structlog.contextvars.merge_contextvars,
#             structlog.processors.TimeStamper(fmt="iso"),
#             structlog.processors.add_log_level,
#             structlog.processors.JSONRenderer(),
#         ]
#     )


import logging
from logging.handlers import TimedRotatingFileHandler
import structlog
import os

def configure_logging():
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    handler = TimedRotatingFileHandler(
        filename=os.path.join(log_dir, "app.log"),
        when="midnight",
        interval=1,
        backupCount=7,          # keep last 7 days
        encoding="utf-8",
        utc=True
    )

    formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(handler)

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.processors.JSONRenderer(),
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

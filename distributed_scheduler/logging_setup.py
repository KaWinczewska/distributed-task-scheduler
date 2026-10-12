import logging
import os


def configure_logging(component: str) -> None:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format=
        f"%(asctime)s %(levelname)-8s {component} [%(threadName)s] %(message)s",
        datefmt="%H:%M:%S",
    )

import logging
import sys

def configure_logging(level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger("migration")
    if logger.handlers:
        return logger
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    ch = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    fh = logging.FileHandler("logs/app.log")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger

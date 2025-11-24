import os
import logging
import json
from datetime import datetime


def load_config(path=None):
    default = {
        "log_dir": "logs",
        "level": "INFO",
        "console": True,
        "filename_prefix": "scrape",
    }
    if not path:
        return default
    try:
        with open(path, 'r', encoding='utf-8') as f:
            cfg = json.load(f)
            if not isinstance(cfg, dict):
                return default
            default.update(cfg)
            return default
    except Exception:
        return default


def setup_logger(name: str = 'amazon_scraper', base_dir: str = None, config_path: str = None, caller_file: str = None):
    cfg = load_config(config_path)
    # Resolve log directory relative to base_dir if provided and log_dir is not absolute
    log_dir = cfg.get('log_dir', 'logs')
    if base_dir and not os.path.isabs(log_dir):
        log_dir = os.path.join(base_dir, log_dir)
    os.makedirs(log_dir, exist_ok=True)

    level = getattr(logging, cfg.get('level', 'INFO').upper(), logging.INFO)
    # Determine filename prefix: prefer config, otherwise use caller file or script name
    prefix = cfg.get('filename_prefix')
    if not prefix:
        # try caller_file then sys.argv[0]
        import sys, inspect
        cf = caller_file
        if not cf:
            try:
                # inspect stack: previous frame
                frm = inspect.stack()[1]
                cf = frm.filename
            except Exception:
                cf = None
        if not cf:
            cf = sys.argv[0] if sys.argv and sys.argv[0] else name
        try:
            prefix = os.path.splitext(os.path.basename(cf))[0]
        except Exception:
            prefix = name
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = os.path.join(log_dir, f"{prefix}_{timestamp}.log")

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Avoid adding duplicate handlers if already configured
    if logger.handlers:
        return logger

    fmt = f"{os.path.basename(filename)} | %(asctime)s | %(levelname)s | %(message)s"
    formatter = logging.Formatter(fmt, datefmt="%Y-%m-%d %H:%M:%S")

    fh = logging.FileHandler(filename, encoding='utf-8')
    fh.setLevel(level)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    if cfg.get('console', True):
        sh = logging.StreamHandler()
        sh.setLevel(level)
        sh.setFormatter(formatter)
        logger.addHandler(sh)

    return logger

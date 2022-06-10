import sys
import logging


def get_logger(name):
    return logging.getLogger(name)


def configure_stdout_logger(level='ERROR'):
    root = logging.getLogger()
    root.setLevel(logging.getLevelName(level))
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    root.addHandler(handler)
    return root

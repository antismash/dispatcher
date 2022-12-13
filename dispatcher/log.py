"""Logging configuration"""

import logging

core_logger = logging.getLogger('dispatcher.core')


def setup_logging():
    logging.basicConfig(format='%(levelname)-7s %(asctime)s   %(message)s',
                        level=logging.DEBUG, datefmt="%d/%m %H:%M:%S")

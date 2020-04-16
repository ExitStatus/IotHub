
__author__ = 'Steve Sherwood'

import logging

def CreateLogger(name, logToConsole = logging.DEBUG, logToFile = logging.DEBUG):
    """Creates and configures a logger"""

    # create logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # create file handler logger if required
    if logToFile is not None:
        fh = logging.FileHandler('%s.log' % name)
        fh.setLevel(logToFile)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    # create console handler logger if required
    if logToConsole is not None:
        # create console handler with a higher log level
        ch = logging.StreamHandler()
        ch.setLevel(logToConsole)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    return logger

def GetLogger(name):
    return logging.getLogger(name)
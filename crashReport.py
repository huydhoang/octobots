# for logging uncaught errors when running exe file
import logging
import os
from os import path

def crashreport():
    LOG_FILENAME = 'crash.log'
    if path.exists(LOG_FILENAME):
        os.remove(LOG_FILENAME)
    logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG)
    logging.exception('Caught an error when running compiled .exe file!')
    
    return 0
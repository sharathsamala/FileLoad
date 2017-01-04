#***************************************************************************************
#* Class wrapper for capturing logs on cosole as well as log file rotated on daily basis
#* their output
#*
#* Author: Kapil Sharma (ks515n)
#* Date: 06/14/2016
#***************************************************************************************

import logging
from logging import handlers

class LogUtil:
    'Wrapper for creating a logger which logs to console and a file both'
    
    def __init__(self, loggerName, logFilePath):
        self.loggerName = loggerName
        self.logFilePath = logFilePath
        
    def get(self):
        logger = logging.getLogger(self.loggerName)
        logger.setLevel(logging.INFO)
        
        # create timed rotating file handler which logs to the file
        fh = logging.handlers.TimedRotatingFileHandler(self.logFilePath, when="midnight", interval=1, backupCount=0)
        fh.setLevel(logging.INFO)
        
        # create console handler which prints log messages to console
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        
        # create formatter and add it to the handlers
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        
        # add the handlers to the logger
        logger.addHandler(fh)
        logger.addHandler(ch)
        
        return logger

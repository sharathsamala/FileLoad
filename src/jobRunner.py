'''
Created on Dec 30, 2016

@author: SC00359878
'''
import os
import commands
import traceback

from util.logUtil import LogUtil
from util.oracleDbUtil import OracleDbUtil
from util.configLoader import getConfig

scriptName='JobRunner'
cfgName='jobRunner.cfg'
configPath = os.getcwd() + '/' + cfgName


logPath='D:\\sc00359878\\ATT\\CARAT\\workspace5\\FileLoaderSpark\\logs\\' + scriptName + '.log'


def main():
    
    #Initialize logger
    logObj = LogUtil(scriptName, logPath)
    logger = logObj.get()
    logger.info('Sit back and relax! Awesomeness is about to unfold.')
    
    #get Config Parameters 
    params = getConfig(configPath, logger)
    logger.info('Starting main module: ' + params['jobName'])
    logger.info('Entering infinite loop')
    
    #get db Connection
    db = OracleDbUtil(params['url'], params['username'], params['password'], logger)
    
    if not db.connect():
        logger.error("unable to connect to oracle")
        
    db.execute('select sysdate from dual')
    logger.info(db.getResult())
    
    extract_id= 1 + db.getExtractId()
    logger.info('extract_id = ' + str(extract_id))
    db.insertSparkLogtable(extract_id, params['jobName'], 'move_to_hdfs', 'Start', 'no exception')
    try:
        commands.getoutput('hadoop fs -mkdir /home/ds272q/input/* /user/ds272q/transfer/extract_id')
        hdir_list = commands.getoutput('hadoop fs -put /home/ds272q/input/* /user/ds272q/transfer/extract_id/')
    except Exception:
        logger.error(traceback.print_exc())
        db.updateSparkLogtable(extract_id, params['jobName'], 'move_to_hdfs', 'Error', 'no exception')
    
    logger.info('completed run')
    
    
if __name__ == "__main__":
    main()

#**************************************************************************
#* Class wrapper for connecting to Oracle and executing queries against it 
#*
#* Author: sharathsamala
#* Date: 12/30/2016
#**************************************************************************
import traceback
import os

from java.lang import Class
from java.sql import DriverManager
from java.sql import Connection
from java.sql import Statement
from java.sql import PreparedStatement
from java.sql import ResultSet
from java.util import Properties


class OracleDbUtil:
    'Wrapper for creating a Oracle DB connection and executing SQL commands over it'
    
    def __init__(self, url, username, password, logger):
        self.logger = logger
        self.username = username
        self.password = password
        self.url = url
        self.connection = None
        self.result = []
        self.error = None
    
    def importJar(self, jarFile):
        '''
        import a jar at runtime (needed for JDBC [Class.forName])
    
        adapted from http://forum.java.sun.com/thread.jspa?threadID=300557
        '''
        from java.net import URL, URLClassLoader
        from java.lang import ClassLoader
        from java.io import File
        
        m = URLClassLoader.getDeclaredMethod("addURL", [URL])
        m.accessible = 1
        m.invoke(ClassLoader.getSystemClassLoader(), [File(jarFile).toURL()])
    
    def connect(self):
        try:
            self.logger.info('Connecting to Oracle DB : ' + self.username)
            
            self.logger.info(os.getcwd() + '/ojdbc14.jar')
            
            self.importJar(os.getcwd() + '/ojdbc14.jar')
            
            Class.forName('oracle.jdbc.OracleDriver')
            
            usernam=self.username
            pwd=self.password
            urlDb=self.url
            self.connection = DriverManager.getConnection(urlDb, usernam, pwd)
            self.connection.setAutoCommit(True)
            self.logger.info('Connected to Oracle DB ')
        except:
            self.error = traceback.format_exc()
            return False
        
        return True
        
    def disconnect(self):
        self.logger.info('Disconnecting from Oracle DB : ' + self.username)
        
        if self.connection is not None:
            self.connection.close()
        self.connection = None
        self.result = []
    
    def getResult(self):
        return list(self.result)
    
    def getExtractId(self):
        
        try : 
            statement = None
            rs = None
            statement = self.connection.createStatement()
            rs = statement.executeQuery("Select max(extract_id) as extract_id from Spark_FileLoad_Log")
            
            while rs.next() : 
                return (rs.getInt(1))
            
        except :
            self.logger.error('Error while fetching extract_id')    
    
    def insertSparkLogtable(self, extract_id, job_name, processname, status, exception_log):
        
        
        sql = 'insert into Spark_FileLoad_Log values ( ?,?,?,?,?)'
        
        self.logger.info('updating sparklog table')  
        try :
            statement = None
            statement = self.connection.prepareStatement(sql)
            statement.setString(1,job_name)
            statement.setString(2,processname)
            statement.setString(3,status)
            statement.setString(4,exception_log)
            statement.setInt(5,extract_id)
            
            statement.executeUpdate()
            self.connection.commit()
        except :
            self.logger.error('error while updating sparklog table')  
            
            
    def updateSparkLogtable(self, extract_id, job_name, processname, status, exception_log):
        
        sql = ' update Spark_FileLoad_Log set  ProcessName= ? , Status= ? , Exception_Log=?  where Extract_id = ? and Job_name= ?'
        self.logger.info('Executing sql: ' + sql)
        
        try :
            statement = None
            statement = self.connection.prepareStatement(sql)
            statement.setString(1,processname)
            statement.setString(2,status)
            statement.setString(3,exception_log)
            statement.setInt(4,extract_id)
            statement.setString(5,job_name)
            statement.executeUpdate()
            self.connection.commit()
        except :
            self.logger.error('error while updating sparklog table')  

    '''
    @attention: Column values are returned as string type and requires appropriate casting 
    in the caller. Null values can be compared as NULL string type
    '''
    def execute(self, sql, isDML = False, logSql = True):
        if logSql:
            self.logger.info('Executing sql: ' + sql)
        
        self.result = []
        statement = None
        rs = None
        
        try:
            statement = self.connection.createStatement()
            
            if isDML:
                statement.executeUpdate(sql)
            else:
                rs = statement.executeQuery(sql)
                metadata = rs.getMetaData()
                
                while rs.next():
                    i = 0
                    columns = []
                    
                    'Get all columns from a row'
                    while i < metadata.getColumnCount():
                        columns.append('NULL' if rs.getString(i + 1) is None else rs.getString(i + 1))
                        i += 1
                        
                    'Put that row in our result as a line with column values separated by tabs'
                    self.result.append('\t'.join(columns))
        except:
            self.error = traceback.format_exc()
            return False
        else:
            if rs is not None:
                rs.close()
            if statement is not None:
                statement.close()
            
        return True

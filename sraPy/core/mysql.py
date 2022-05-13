#!/usr/bin/env python
# coding=utf-8

import os, prefect, pendulum
import pandas as pd
import numpy as np
from prefect.tasks.secrets import PrefectSecret
from prefect.tasks.aws import AWSSecretsManager
from pymysql import Connect, MySQLError

import time, warnings
warnings.simplefilter(action='ignore',category = UserWarning)

# --------------------------------------------------------------------------------------------------
# mysql class

class MySQLController(object):
    """
        Python Class for connecting  with MySQL servers and querying,updating, or deleting using MySQL
    """

    __instance = None
    __session = None
    __connection = None
    __run_type = None
    __schema = None
    __config = None
    __logger = prefect.context.get('logger')

    def __init__(self, run_type = 'dev', schema = None):
        warnings.simplefilter(action='ignore',category = UserWarning)
        if not schema:
            self.__logger.error('\n PLEASE SELECT A SCHEMA!\nTry SRA or SRSE\n')
        else:
            self.__run_type = run_type
            self.__schema = schema.upper()
            self.__auth()

    ## End def __init__

    def __auth(self):
        cred = PrefectSecret("AWS_CREDENTIALS")
        auth = AWSSecretsManager(boto_kwargs = { "region_name":"us-east-2" }).run(secret = "MYSQL_DB_AUTH")
        db_config = auth[self.__run_type]
        self.__config = db_config[self.__schema]

    ## End def __auth

    def __open(self):
        try:
            if self.__connection:
                self.__connection.ping(reconnect=True)
                self.__session = self.__connection.cursor()
            else:
                cnx = Connect(**self.__config)
                self.__connection = cnx
                self.__session = cnx.cursor()
        except MySQLError as e:
            print("Error %d: %s" % (e.args[0], e.args[1]))

    ## End def __open

    def __close(self):
        try:
            if self.__connection:
                if self.__connection.open:
                    self.__connection.close()
            else:
                print('No connection to close.')
        except MySQLError as e:
            print("Error %d: %s" % (e.args[0], e.args[1]))

    ## End def __close

    def __check_table_locks(self, db='sradb', table=None, maxRetries=5, waitTime=10):
        """
        check if table is locked
        example usage:
        if fnCheckTableLocks(connSRA, db='sradb', table='testtable'):
            print('Connected')
            do_other_stuff()
        """

        if not table:
            print('Please choose a table name')
            return

        db = db.lower()
        table = table.lower()
        maxRetries = maxRetries + 1

        self.__open()

        # for beta sheets, just check if table exists
        if table.startswith('beta_sheet'):
            query = """
                        SELECT COUNT(*) as existingTbl
                        FROM information_schema.tables
                        WHERE table_schema = '%s'
                        AND table_name = '%s'
                        ;
                    """ % (db, table)

            for i in range(maxRetries):
                checkExists = pd.read_sql(query, self.__connection)['existingTbl']

                if checkExists.values == 1:
                    return True
                if i == maxRetries-1:
                    self.__logger.error('===== MAXIMUM RETRIES EXCEEDED =====')
                    self.__logger.error('FAILURE CONNECTING TO: %s.%s' % (db, table))
                    break
                else:
                    self.__logger.warning('Missing table %s.%s. Waiting %s seconds to reattempt %d of %s.' %
                                    (db, table, waitTime, i+1, maxRetries-1))
                    time.sleep(waitTime)

        ## for all other tables, check In_use status
        else:
            query = """SHOW OPEN TABLES in %s WHERE In_use = 0""" % db

            # loop through checking if table is locked
            for i in range(maxRetries):
                checkLock = pd.read_sql(query, self.__connection)['Table']
                if table in checkLock.values:
                    return True
                if i == maxRetries-1:
                    self.__logger.error('===== MAXIMUM RETRIES EXCEEDED =====')
                    self.__logger.error('FAILURE CONNECTING TO: %s.%s' % (db, table))
                    break
                else:
                    self.__logger.warning('Missing table %s.%s. Waiting %s seconds to reattempt %d of %s.' %
                                    (db, table, waitTime, i+1, maxRetries-1))
                    time.sleep(waitTime)
            self.__close()
        return True

    ## end def check_table_locks

    def query(self, select="*", db=None, table=None, where=None, *args, **kwargs):

        strSELECT = "SELECT "
        strWHERE = " "

        lstVals=[]
        if not type(select)==list:
            for val in select.split(","):
                lstVals.append(val.strip(" "))
            select = lstVals

        strSELECT += ", ".join(select)
        strFROM = f" FROM {db}.{table} "
        if where:
            if not type(where)==list:
                where = [where]
            for constraint in tuple(where):
                strWHERE += f" AND {constraint}"

        query = f"""
                {strSELECT}
                {strFROM}
                WHERE 1=1
                {strWHERE}
            """

        self.__open()
        if kwargs:
            result = pd.read_sql_query(query, self.__connection, **kwargs)
        else:
            result = pd.read_sql_query(query, self.__connection)
        self.__close()
        return result

    ## End def simple_query

    def advanced_query(self, query = None, *args, **kwargs):
        self.__open()
        result = pd.read_sql_query(query, self.__connection, **kwargs)
        self.__close()
        return result

    ## End def advanced_query

    def cursor_execute(self, exec_query=None, *args, **kwargs):
        self.__open()
        self.__logger.info(f'----- EXECUTING QUERY: -----\n{exec_query}\n')
        try:
            self.__session.execute(exec_query)
        except Exception as e:
            self.__logger.error(e, exc_info = True)
        finally:
            self.__close()
        return

    ## End def cursor_execute

    def upload(self, df = None, db = None, table = None, mode = 'REPLACE', colNames = None, unlinkFile = True):

        from sraPy.common import setOutputFilePath

        dttm = pendulum.now().to_datetime_string().replace('-', '_').replace(':', '_').replace(' ', '-')
        tmpFile = setOutputFilePath(OUTPUT_SUBDIRECTORY = 'upload', OUTPUT_FILE_NAME = f"{table}_{dttm}.txt")

        self.__logger.info(f"Creating temp file: {tmpFile}")
        self.__open()
        colsSQL = pd.read_sql(f"""SELECT * FROM {db}.{table} LIMIT 0;""", self.__connection).columns.tolist()
        self.__close()

        if colNames:
            # check columns in db table vs dataframe
            colsDF = df[colNames].columns.tolist()
            colsDiff = set(colsSQL).symmetric_difference(set(colsDF))

            if len(colsDiff) > 0:
                self.__logger.warning(f'----- COLUMN MISMATCH WHEN ATTEMPTING UPLOAD TO {table} -----')
                if len(set(colsDF) - set(colsSQL)) > 0:
                    self.__logger.warning(f'Columns in dataframe not found in {db}.{table}: \n{list((set(colsDF) - set(colsSQL)))}')
                else:
                    df[colsDF].to_csv(tmpFile, sep = "\t", na_rep = "\\N", float_format = "%.8g", header = True, index = False, doublequote = False)
                    query = """LOAD DATA LOCAL INFILE '%s' %s INTO TABLE %s.%s LINES TERMINATED BY '\n' IGNORE 1 LINES (%s)""" % \
                            (tmpFile.replace('\\', '/'), mode, db, table, colsDF)

                    self.__logger.debug(query)
                    self.__open()
                    rv = self.__session.execute(query)
                    self.__close()
                    self.__logger.info("Number of rows affected: %s" % len(df))
                    return

        # check columns in db table vs dataframe
        colsDF = df.columns.tolist()
        colsDiff = set(colsSQL).symmetric_difference(set(colsDF))

        if len(colsDiff) > 0:
            self.__logger.warning('----- COLUMN MISMATCH WHEN ATTEMPTING TO UPLOAD %s -----' % table)
            if len(set(colsSQL) - set(colsDF)) > 0:
                self.__logger.warning('Columns in %s.%s not found in dataframe: %s' % (db, table, list((set(colsSQL) - set(colsDF)))))
            if len(set(colsDF) - set(colsSQL)) > 0:
                self.__logger.warning('Columns in dataframe not found in %s.%s: %s' % (db, table, list((set(colsDF) - set(colsSQL)))))

        df[colsSQL].to_csv(tmpFile, sep = "\t", na_rep = "\\N", float_format = "%.8g", header = True, index = False, doublequote = False)
        query = """LOAD DATA LOCAL INFILE '%s' %s INTO TABLE %s.%s LINES TERMINATED BY '\n' IGNORE 1 LINES""" % \
                (tmpFile.replace('\\', '/'), mode, db, table)

        self.__logger.debug(query)
        self.__open()

        rv = self.__session.execute(query)

        self.__close()
        self.__logger.info("Number of rows affected: %s" % len(df))

        if unlinkFile:
            os.unlink(tmpFile)
            self.__logger.info("Deleting temporary file: {}".format(tmpFile))
        self.__logger.info("DONE")
        return

    ## End def upload
## End class

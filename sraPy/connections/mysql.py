#!/usr/bin/env python
# coding=utf-8

import os, prefect, pendulum
import pandas as pd
import numpy as np
from prefect.tasks.secrets import PrefectSecret
from prefect.tasks.aws import AWSSecretsManager
from pymysql import Connect, MySQLError
from sraPy.core.mysql import MySQLController

import time, warnings
warnings.simplefilter(action='ignore',category = UserWarning)

# --------------------------------------------------------------------------------------------------
# mysql class

# --------------------------------------------------------------------------------------------------
# srse controller class

# noinspection SqlNoDataSourceInspection

class srseController(MySQLController):

    def __init__(self, run_type = 'dev'):
        self.__schema = "SRSE"
        # todo:
        self.__run_type = run_type
        super().__init__(self.__run_type, self.__schema)


# --------------------------------------------------------------------------------------------------
# sradb controller class

# noinspection SqlNoDataSourceInspection
class sraController(MySQLController):

    def __init__(self, run_type = 'dev'):
        self.__schema = "SRA"
        self.__run_type = run_type
        super().__init__(self.__run_type, self.__schema)

    def get_strategyFocus(self, df=None,dateMin=None,dateMax=None,*args, **kwargs):

        accnts = df['accnt'].unique().tolist()

        # load strategy risk
        dbName = 'sradb'
        msgName = 'tblperformance_daily'

        strORDER = """  
                                accnt,
                                strategy,
                                strategyLevel,
                                date
                    """

        strWHERE = ' '

        if dateMin:
           strDateMIN = " AND date >= ('%s')" % dateMin
           strWHERE = strWHERE + strDateMIN

        if dateMax:
           strDateMAX = " AND date <= ('%s')" % dateMax
           strWHERE = strWHERE + strDateMAX

        if accnts:
            strACCNT = " AND accnt IN ('%s')" % "', '".join(accnts)
            strWHERE = strWHERE + strACCNT

        tblName = "%s.%s" % (dbName, msgName)

        query = """
                    SELECT * 
                    FROM %s 
                    WHERE 1=1 %s
                    AND substr(accnt,1,1) != 'T'
                    AND aggLevel = 'Strategy'
                    ORDER BY %s
                    ;
                """ % (tblName, strWHERE, strORDER)

        self._MySQLController__open()
        result = pd.read_sql_query(query, self._MySQLController__connection,**kwargs)
        self._MySQLController__close()

        df['focusKey'] = df[['accnt','strategy','strategyLevel','date']].astype('str').agg(':'.join,axis=1)
        result['focusKey'] = result[['accnt','strategy','strategyLevel','date']].astype('str').agg(':'.join,axis=1)
        dfResult = pd.merge(df.set_index('focusKey'),result.set_index('focusKey')['strategyFocus'], how='left',on='focusKey').reset_index(drop=True)

        return dfResult

    def map_daily_mandates(self, df=None,dateMin=None,dateMax=None,*args, **kwargs):

        accnts = df['accnt'].unique().tolist()

        # load strategy risk
        dbName = 'sradb'
        msgName = 'tblperformance_daily'

        strORDER = """  
                                accnt,
                                strategy,
                                strategyLevel,
                                date
                    """

        strWHERE = ' '

        if dateMin:
           strDateMIN = " AND date >= ('%s')" % dateMin
           strWHERE = strWHERE + strDateMIN

        if dateMax:
           strDateMAX = " AND date <= ('%s')" % dateMax
           strWHERE = strWHERE + strDateMAX

        if accnts:
            strACCNT = " AND accnt IN ('%s')" % "', '".join(accnts)
            strWHERE = strWHERE + strACCNT

        tblName = "%s.%s" % (dbName, msgName)

        query = """
                    SELECT * 
                    FROM %s 
                    WHERE 1=1 %s
                    AND substr(accnt,1,1) != 'T'
                    AND aggLevel = 'Strategy'
                    ORDER BY %s
                    ;
                """ % (tblName, strWHERE, strORDER)

        self._MySQLController__open()
        result = pd.read_sql_query(query, self._MySQLController__connection,**kwargs)
        self._MySQLController__close()

        df['key'] = df[['accnt','strategy','strategyLevel','date']].astype('str').agg(':'.join,axis=1)
        result['key'] = result[['accnt','strategy','strategyLevel','date']].astype('str').agg(':'.join,axis=1)

        dfResult = pd.merge(df.set_index('key'),result.set_index('key')['startMandate'], how='left',on='key').reset_index(drop=True)
        dfResult['startMandate'] = dfResult['startMandate'].replace(0, np.nan)
        dfResult.sort_values('date')['startMandate'].fillna(method = 'ffill', inplace = True)
        return dfResult

    def get_config(self, configType = 'account',*args, **kwargs):
        db = "sradb"
        msgName = f"msgSRA_{configType}config"
        query = f"""
                SELECT *
                FROM
                {db}.{msgName}
            """

        self._MySQLController__open()
        result = pd.read_sql_query(query, self._MySQLController__connection,**kwargs)
        self._MySQLController__close()

        return result

    def get_risk_table(self, table = 'beta_sheet_strategy_risk', days = None, accnts = None, tickers = None, noTest = True, *args, **kwargs):
        """
         'beta_sheet_strategy_risk'
        'beta_sheet_daily_strategy_risk'
        'beta_sheet_daily_strategy_risk_bf'
        'beta_sheet_intra_strategy_risk'
        """
        db = "sradb"
        msgName = table
        strWHERE = ' '

        if (noTest):
            strTEST = " AND substr(accnt,1,1) != 'T'"
            strWHERE = strWHERE + strTEST
        if days:
            strDAYS = " AND DATE >= DATE_SUB(CURDATE(), INTERVAL %s DAY)" % days
            strWHERE = strWHERE + strDAYS
        if accnts:
            strACCNT = " AND accnt IN ('%s')" % "', '".join(accnts)
            strWHERE = strWHERE + strACCNT
        if tickers:
            strSYM = " AND ticker_tk IN ('%s')" % "', '".join(tickers)
            strWHERE = strWHERE + strSYM

        table = "%s.%s" % (db, msgName)

        checkTbl = self._MySQLController__check_table_locks(db='sradb', table=msgName)
        self._MySQLController__open()

        if checkTbl:
            try:
                query = f"""
                            SELECT *
                            FROM {table}
                            WHERE 1=1 {strWHERE}
                            ;
                        """

                result = pd.read_sql_query(query, self._MySQLController__connection, **kwargs)
                if (pd.isnull(len(result)) or len(result) == 0):
                    print("%s %s: " % (self.__connection, table))
                    print("SQL SELECT ERROR")
                    print("statement: %s" % query)
                    print("result: ")
                    print(result)
                    print("")
                    return
                else:
                    return result
            finally:
                self._MySQLController__close()

    # # end load_risk_table def
# end class def

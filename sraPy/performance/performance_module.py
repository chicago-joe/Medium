# -*- coding: utf-8 -*-
"""
Created on Wed Jun 19 10:32:24 2019

@author: mark.lutz
"""

import os, sys, logging
import json, requests, datetime, time
import pandas as pd
pd.set_option('mode.chained_assignment',None)

import numpy as np
import re, warnings, multiprocessing
from multiprocessing.pool import ThreadPool
from functools import partial

#todo:
#import and login to tiingo
from tiingo import TiingoClient
config = { }
config['session'] = True
# joe.loss api key
config['api_key'] = '473f1019b1f05c17a44ac39484a1ad8129d597ac'
client = TiingoClient(config)

warnings.simplefilter('ignore', category=FutureWarning)
warnings.simplefilter('ignore', category=DeprecationWarning)

from sraPy.finance.loadHistorical import fnGetIndexReturns, fnComputeTotalReturn
from sraPy.common import setOutputFilePath, setLogging
from sraPy.connections.mysql import sraController, MySQLController


# --------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------
# INPUTS

inputs = {
         'accnt'                : 'A.F.848',  # string - delimited list
         'strategy'             : None,  # string - delimited list
         'institutionalAdvisor' : None,  # string - delimited list
         'startDate'            : None,  # string - 'YYYY-MM-DD'
         'endDate'              : None,  # string - 'YYYY-MM-DD'
         'shadow'               : None,  # SPY, treasury
         'benchmark'            : None,  # SPY, PUT, BXM
         'fees'                 : False, # Bool
         'underlyingTicker'     : None,  # string - delimited list
         'strategyFocus'        : None,  # string - delimited list
         'aggregateAccnts'      : False, # Bool
         'aggregateStrats'      : False, # Bool
         'aggregateTickers'     : False, # Bool
         'composite'            : False, # Bool
         'output'               : None,  # monthly, daily
         'advisorDetail'        : False  # Bool
         }

ignore_list = ['A.K']

# --------------------------------------------------------------------------------------------------
# CONSTANTS

URL_INTERNAL = 'http://10.37.2.190:8080/performance-report/generate-pdf'

POSITIONPNL_KEY = ['date', 'accnt', 'secKey_at', 'secKey_tk', 'secKey_yr', 'secKey_mn', 'secKey_dy', 'secKey_xx', 'secKey_cp', 'strategy', 'strategyLevel']

today_date = datetime.datetime.combine(datetime.date.today(), datetime.time.min)


# --------------------------------------------------------------------------------------------------
# HELPER FUNCTIONS

def parallelize_dataframe(df, df_sc, func):

    num_cores = min(multiprocessing.cpu_count() - 2, 4) # gurantees at least 2 free cores
    num_partitions = num_cores * 5
    df_split = np.array_split(df, num_partitions)
    pool = multiprocessing.Pool(num_cores)
    #df = pd.concat(pool.starmap(func, [df_split, df_sc]))
    df = pd.concat(pool.map(partial(func, df_sc), df_split))
    pool.close()
    pool.join()
    return df


def gen_filt(inputs, *args, s_lead = 'WHERE'):
    """Sql query condition construction function to help make account or strategy level queries.

    Args:
        s_lead (str, optional): Leading string for sql condition. Default 'WHERE'.

    Notes:
        Delimiters can be space, comma, semi-colon, line break, or tab.

    Returns:
        str: Sql query condition.
    """

    l_filt = []

    # live vs non_live
    if 'startDate' in args:
        if inputs['startDate'] is not None:
            l_filt.append("date >= '%s'" % inputs['startDate'])

    if 'endDate' in args:
        if inputs['endDate'] is not None:
            l_filt.append("date <= '%s'" % inputs['endDate'])

    for arg in args:
        if arg not in {'startDate', 'endDate', 'fees'}:
            filter_in_list(arg, inputs[arg], l_filt)

    if len(l_filt) > 0:
        return s_lead + ' ' + ' AND '.join(l_filt)
    else:
        return ''


def str_to_list(f_input, sep):
    """Converts a string representation of a list into a python list of strings.

    Args:
        f_input (str): String representation of a list. See Note.

    Notes:
        Delimiters can be space, comma, semi-colon, line break, or tab.

    Returns:
        list: List of strings.
    """

    delim = ' |,|;|\n|\t'
    return list(set([sep + "%s" % sub.strip() + sep for sub in re.split(delim, f_input) if sub.strip() != '']))


def filter_in_list(field, f_input, l_filt):
    """Appends to list of sql conditions based on a sql 'IN' filter.

    Args:
        field (str): Name of field.
        f_input (str): String representation of a list.
        l_filt (list of str): List of basic sql conditions (excludes AND's and WHERE's)
    """

    if f_input is None or f_input == '':
        return

    str_list = str_to_list(f_input, "'")

    if len(str_list) > 0:
        l_filt.append("%s IN (%s)" % (field, ','.join(str_list)))


def filter_like(field, f_input, l_filt):
    """Appends to list of sql conditions based on a sql 'LIKE' filter.

    Args:
        field (str): Name of field.
        f_input (str): String representation of a pattern used in sql LIKE condition.
        l_filt (list of str): List of basic sql conditions (excludes AND's and WHERE's)
    """

    if f_input is None or f_input == '':
        return

    l_filt.append("%s LIKE '%s'" % (field, f_input))


def quarter_start_end_next(dt_in):

    q_dt = np.array([datetime.datetime(dt_in.year + yr_off, mn, dy)
                     for yr_off in [-1, 0, 1]
                     for mn, dy in [(3, 31), (6, 30), (9, 30), (12, 31)]])

    q_dt_a = q_dt[q_dt <= dt_in]

    return q_dt[q_dt < dt_in][-1], q_dt_a[0], q_dt_a[1]


def unpack_kwargs(func):

    def new_func(kwargs):

        return func(**kwargs)

    return new_func


def filter_df_period(df, row, grpLevel, srsBool, period = None):

    df_lvl = df.copy()
    for lvl in grpLevel:
        df_lvl = df_lvl[df_lvl[lvl] == row[lvl]]

    if period is not None:
        df_lvl = df_lvl[df_lvl['period'] == period]

    if df_lvl.empty:
        return None
    elif srsBool == True:
        return df_lvl.iloc[0]
    else:
        return df_lvl


def convert_to_dict(df):

    lst_stock = []
    lst_total = []
    for index, row in df.iterrows():
        dict_stock = {"_date": row['date'],
                       "_value": row['SDRtr']}
        dict_total = {"_date": row['date'],
                       "_value": row['TRtr']}
        lst_stock.append(dict_stock)
        lst_total.append(dict_total)

    return lst_stock, lst_total


# DATA FUNCTIONS


def get_table_trading_date(eng_sra200):

    sql_q = """
            SELECT date AS DateCal, TradingFlg
            FROM sradb._tbldates
            WHERE date BETWEEN '2016-01-01' AND '%s-12-31'
            ;
            """ % datetime.datetime.today().year
    df_c = eng_sra200.advanced_query(sql_q)

    # manual adjustment for special holiday on 12/5/2018
    df_c.loc[df_c['DateCal'] == datetime.date(2018, 12, 5), 'TradingFlg'] = 0

    df_c['DateCal'] = pd.to_datetime(df_c['DateCal'])
    df_c = df_c.sort_values('DateCal')
    df_c['Year'] = df_c['DateCal'].dt.year
    df_c['Quarter'] = pd.PeriodIndex(df_c['DateCal'], freq='Q').strftime('%q').astype(int)
    df_c['YearDay'] = df_c.groupby('Year').cumcount() + 1
    df_c['QuarterDay'] = df_c.groupby(['Year', 'Quarter']).cumcount() + 1

    return df_c


def get_account_config(eng, inputs, *args):

    filt = gen_filt(inputs, *args, s_lead = 'AND')
    query = """
            SELECT *
            FROM sradb.msgsra_accountconfig
            WHERE accnt LIKE 'A.%%'
            AND startDate <> '1900-01-01'
            %s
            ;
            """ % filt

    df_ac = eng.advanced_query(query)

    return df_ac


def get_strategy_config_hist(eng, inputs, *args):

    filt = gen_filt(inputs, *args)
    query = """
            SELECT *
            FROM sradb.strategyconfig
            %s
            ;
            """ % filt

    df_sc_hist = eng.advanced_query(query)

    return df_sc_hist


def get_historical_prices(start_date, end_date, lst_tk = None):

    # grab historical adjusted prices

    if isinstance(lst_tk, str):
        if lst_tk == 'BIL':
                df_tk_h = fnComputeTotalReturn(start_date, end_date, lst_tk).rename('BIL')
                df_tk_h = pd.DataFrame(df_tk_h).reset_index().rename(columns = {'Date' : 'date'})

    elif lst_tk is not None:
        df_tk_h = client.get_dataframe(lst_tk,
                                       frequency   = 'daily',
                                       metric_name = 'adjClose',
                                       startDate   =  start_date,
                                       endDate     =  end_date)\
                        .reset_index()\
                        .rename(columns={'index':'date'})

    else:
        df_tk_h = fnGetIndexReturns(start_date, end_date).reset_index().rename(columns = {'Date' : 'date'})

    for col in df_tk_h.columns[1:]:
        df_tk_h[col] = df_tk_h[col].pct_change()

    # remove timezone using tz_localize
    df_tk_h['date'] = pd.to_datetime(df_tk_h.date).dt.tz_localize(None)

    return df_tk_h


def get_benchmark_prices(eng, inputs, *args):

    start_date = datetime.datetime.strftime(datetime.datetime.strptime(inputs['startDate'], '%Y-%m-%d') - datetime.timedelta(7), '%Y-%m-%d')

    filt = gen_filt(inputs, *args, s_lead = 'AND')
    query = """
            SELECT *
            FROM sradb.indexprices
            WHERE date >= '%s'
            %s
            ;
            """ % (start_date, filt)

    df_bp = eng.advanced_query(query)
    df_bp['spyReturn'] = df_bp['SPYPrice'].pct_change()

    return df_bp.rename(columns = {'Date': 'date'})


def get_positionpnl(eng, inputs, *args):

    filt = gen_filt(inputs, *args)
    query = """
            SELECT *
            FROM sradb.tblpositionpnl_bf
            %s
            ;
            """ % filt

    df_pp = eng.advanced_query(query)

    return df_pp


def oye_cond(df):
    return (
                   (df['strategy'] == 'OYE') | \
                   (df['strategy'] == 'OYENT')
           ) & (
                   (df['allocationPct'] != 1)
           )


def calc_allocation_pct(dr):

    if dr['secKey_cp'] != 'None':

        return 1

    else:

        lst_csc = str(dr['allowedTickersCsc']).replace(',', ';').split(';')
        lst_pct = str(dr['allowedTickersPct']).replace(',', ';').split(';')
        lst_allowed_tickers = str(dr['allowedTickers']).replace(',', ';').split(';')

        if (dr['allowedTickersCsc'] == '' | dr['allowedTickersCsc'] is None) and \
           (dr['allowedTickersPct'] == '' | dr['allowedTickersPct'] is None):

            return dr['allocationPct']

        else:

            if dr['underlyingTicker'] in lst_allowed_tickers:

                ticker_idx = lst_allowed_tickers.index(dr['underlyingTicker'])

                if dr['prevEodPos'] == 0:

                    return 0

                if dr['allowedTickersCsc'] != '':

                    return float(lst_csc[ticker_idx]) / dr['prevEodPos']

                elif dr['allowedTickersPct'] != '':

                    return float(lst_pct[ticker_idx])


def null_per_strategy(dr):

    if dr['strategy'] in ['CSP', 'PUT', 'SNR']:
        dr[['SRtr', 'DRtr', 'SDRtr', 'stockPnl', 'divPnl', 'stockVol', 'volRed']] = None

    if not ((dr['strategy'] == 'SNR' and \
          dr['strategyFocus'] in ['SNR2', 'SNR3', 'SNR4', 'SNR5', 'SNR6', 'SNR7']) or\
         (dr['strategy'] == 'EFR') or \
         (inputs['benchmark'] is not None)):
        dr[['BMRtr', 'bmPnl', 'bmVol']] = None

    if inputs['shadow'] != 'BIL':
        dr[['RRtr', 'ratePnl']] = None

    return dr


def dict_aum_ret_rsk(df, period, ret_pre = ''):

    if period == 'INCEPTION':
        stockSR = df['stockSR']
        totalSR = df['totalSR']
        bmSR = df['bmSR']
    else:
        stockSR = None
        totalSR = None
        bmSR = None

    return    {
                  "_aum": {
                      "_title": "Assets Under Management",
                      "_startSraAccountBalance": df['startMandate'],
                      "_endSraAccountBalance": df['endMandate']
                  },
                  "_ret": {
                      "_title": "Return",
           			  "_equityReturnAmt": df['stockPnl'],
        			  "_equityReturnPct": df['%sSRtr' % ret_pre] if df['%sSRtr' % ret_pre] != 0 else None,
        			  "_optionReturnAmt": df['optPnl'],
        			  "_optionReturnPct": df['%sORtr' % ret_pre] if df['%sORtr' % ret_pre] != 0 else None,
        			  "_dividendReturnAmt": df['divPnl'],
        			  "_dividendReturnPct": df['%sDRtr' % ret_pre] if df['%sDRtr' % ret_pre] != 0 else None,
        			  "_totalReturnAmt": df['totalPnl'],
        			  "_totalReturnPct": df['%sTRtr' % ret_pre] if df['%sTRtr' % ret_pre] != 0 else None,
        			  "_benchmarkReturnAmt": None,
        			  "_benchmarkReturnPct": df['%sBMRtr' % ret_pre] if df['%sBMRtr' % ret_pre] != 0 else None,
        			  "_cashFlowYieldAmt": None,
        			  "_cashFlowYieldPct": None,
        			  "_interestRateReturnAmt": df['ratePnl'],
        			  "_interestRateReturnPct": df['%sRRtr' % ret_pre] if df['%sRRtr' % ret_pre] != 0 else None
                  },
                  "_rsk": {
                      "_title": "Risk",
        			  "_equityVolatility": df['stockVol'],
        			  "_optionVolatility": None,
        			  "_totalVolatility": df['totalVol'],
        			  "_totalVolatilityReduction": df['volRed'],
        			  "_benchmarkVolatility": df['bmVol'],
        			  "_equitySharpeRatio": stockSR,
        			  "_optionSharpeRatio": None,
        			  "_totalSharpeRatio": totalSR,
        			  "_benchmarkSharpeRatio": bmSR
                  },
                  "_period": period
              }


def download_all_files(file_name_json):

    download_file(file_name_json, 'pdf')


def download_file(file_name_json, file_type):

    files = {'data': ('a.json', open(file_name_json, 'rb'), "multipart/form-data")}
    data = {'type': file_type}
    dl_file_name = file_name_json.replace('.json', '.%s' % file_type)

    response = requests.post(URL_INTERNAL, files=files, data=data)
    with open(dl_file_name, 'wb') as open_file:
        for chunk in response.iter_content(chunk_size=128):
            open_file.write(chunk)


def run_performance(inputs, generate_json, freq = None):

    logging.debug(f"Parent Process ID:{os.getppid()}, Process ID: {os.getpid()}")
    if freq == 'monthly':
        inputs['output'] = 'monthly'
        if inputs['startDate'] is not None:
            tmp_startDate = datetime.datetime.strptime(inputs['startDate'], '%Y-%m-%d')
            inputs['startDate'] = str(tmp_startDate.year) + '-' + str(tmp_startDate.month) + '-01'

    logging.debug('Connecting to SRA200.')
    connSRA = sraController(run_type="prod")
    logging.debug('Connecting to SRSE V7.')
    connSRSE = MySQLController(run_type="prod",schema="SRSE")

    logging.debug('Getting current Account Config.')
    df_ac = get_account_config(connSRA, inputs, 'accnt', 'institutionalAdvisor')

    # update accounts based on accnt config results
    if not df_ac.empty:
        for accnt in ignore_list:
            df_ac = df_ac[~df_ac['accnt'].str.contains(accnt)]
        inputs.update({'accnt' : ",".join(df_ac['accnt'].tolist())})
    elif df_ac.empty and (inputs['institutionalAdvisor'] is not None):
        logging.info('No accounts returned for advisor %s. Closing Program...' % inputs['institutionalAdvisor'])
        return

    logging.debug('Getting historical strategy config.')
    df_sc_hist = get_strategy_config_hist(connSRA, inputs, 'accnt', 'strategy')
    max_date = df_sc_hist.groupby(['accnt', 'strategyLevel', 'strategy'])['date'].transform(max)
    df_sc_hist_max = df_sc_hist[df_sc_hist['date'] == max_date]

    # return if df_sc is empty
    if df_sc_hist.empty:
        logging.info(f"No accounts returned for inputs: {inputs.__str__()}")
        return

    # update both startDate and endDate to be minimum start date and today's date if not specified
    if inputs['startDate'] is None:
        minDate = datetime.datetime.strftime(df_sc_hist['startDate'].min(), '%Y-%m-%d')
        inputs.update({'startDate' : minDate})

    if inputs['endDate'] is None:
        maxDate = datetime.datetime.strftime(today_date, '%Y-%m-%d')
        inputs.update({'endDate' : maxDate})

    logging.debug('Getting historical position P&L.')
    df_pp_all = get_positionpnl(connSRA, inputs, 'accnt', 'startDate', 'endDate', 'underlyingTicker', 'strategy')
    #df_pp_all.rename(columns={'secKey_at' : 'ticker_at', 'underlyingTicker' : 'ticker_tk', 'secKey_cp' : 'okey_cp', 'prevEodPos' : 'pos'}, inplace=True)


    # assigning grouping variables based on whether the perf report is for a single accnt/strategy or for an aggregate
    grpLevelAccnt = ['accnt', 'strategyLevel', 'strategy']
    grpLevelAggAccnt = ['strategy', 'strategyFocus']
    grpLevelAggStrat = ['accnt']
    grpLevelAccntHEC = ['accnt', 'strategyLevel', 'strategy', 'underlyingTicker']

    if inputs['aggregateAccnts']:
        grpLevel = grpLevelAggAccnt
        aggLevelNonHEC = 'Composite'
        grpLevelHEC = grpLevelAggAccnt
        aggLevelHEC = 'Composite'
    elif inputs['aggregateStrats']:
        grpLevel = grpLevelAggStrat
        aggLevelNonHEC = 'Account'
        grpLevelHEC = grpLevelAggStrat
        aggLevelHEC = 'Account'
    elif inputs['aggregateTickers']:
        grpLevel = grpLevelAccnt
        aggLevelNonHEC = 'Strategy'
        grpLevelHEC = grpLevelAccnt
        aggLevelHEC = 'Strategy'
    else:
        grpLevel = grpLevelAccntHEC
        aggLevelNonHEC = 'Strategy'
        grpLevelHEC = grpLevelAccntHEC
        aggLevelHEC = 'Ticker'

    h_start_date = datetime.datetime.strftime(datetime.datetime.strptime(inputs['startDate'], '%Y-%m-%d') - datetime.timedelta(7), '%Y-%m-%d')

    # if shadow is specified, find historical data
    if (inputs['shadow'] is not None) and ~((df_sc_hist['basisRiskTicker'].str.contains(inputs['shadow']))).any():
        if inputs['shadow'] == 'BIL':
            df_sh_h = get_historical_prices(h_start_date, inputs['endDate'], 'BIL')
        else:
            lst_tk = str_to_list(inputs['shadow'], '')
            df_sh_h = get_historical_prices(h_start_date, inputs['endDate'], lst_tk)

    else:
        df_sh_h = pd.DataFrame([], columns = ['date'])

    # if benchmark is specified, find historical data
    if inputs['benchmark'] is not None:
        if inputs['benchmark'] in ['BXM', 'PUT']:
            df_bm_h = get_historical_prices(h_start_date, inputs['endDate'])
        else:
            lst_tk = str_to_list(inputs['benchmark'], '')
            df_bm_h = get_historical_prices(h_start_date, inputs['endDate'], lst_tk)

    # if no bm specified, find historical data for SNR allowed tickers
    elif ((df_sc_hist['strategy'] == 'SNR').any()) or ((df_sc_hist['basisRiskTicker'] != '').any()):
        lst_lst_tk = pd.concat([df_sc_hist[df_sc_hist['strategy'] == 'SNR']['allowedTickers'],\
                               df_sc_hist[df_sc_hist['basisRiskTicker'] != '']['basisRiskTicker']])\
                               .str.replace('SPX','SPY').str.replace('XSP','SPY').str.replace('RUT','IWM').str.replace('NDX','QQQ')\
                               .str.replace('.','-').str.split(';').tolist()
        lst_tk = np.unique([item for sublist in lst_lst_tk for item in sublist])
        df_bm_h = get_historical_prices(h_start_date, inputs['endDate'], lst_tk)

    else:
        df_bm_h = pd.DataFrame([], columns = ['date'])

    if ('SPY' not in df_sh_h.columns) and ('SPY' not in df_bm_h.columns):
        df_bm_h = df_bm_h.merge(get_historical_prices(h_start_date, inputs['endDate'], ['SPY']), 'outer', on = 'date')

    spyPrice = client.get_dataframe('SPY',
                                    frequency   = 'daily',
                                    metric_name = 'close',
                                    startDate   =  h_start_date,
                                    endDate     =  inputs['endDate'])\
                     .reset_index()\
                     .rename(columns={'index':'date'})
    spy_1 = spyPrice.copy().rename(columns = {'date':'date_1', 'close':'prevClose'})
    spy_1.index = spy_1.index + 1
    spy = spyPrice.merge(spy_1, 'left', left_index = True, right_index = True)



    if not inputs['aggregateStrats']:
        # attach newest relative strategy config to each positionpnl row
        df_pp_all.sort_values('date', inplace = True)
        is_included = df_pp_all['recordLabel'] == 'Included'
        df_pp_all=df_pp_all.set_index(pd.DatetimeIndex(df_pp_all['date'])).tz_localize('America/Chicago').drop(columns=['date']).reset_index()
        df_sc_hist=df_sc_hist.set_index(pd.DatetimeIndex(df_sc_hist['date'])).tz_localize('America/Chicago').drop(columns=['date']).reset_index()
        df_pp_max_sc = pd.merge_asof(df_pp_all[is_included], df_sc_hist, on = ['date'], by = ['accnt', 'strategy', 'strategyLevel'])


        # if this is a composite:
        #       allow 1 month grace period
        #       exclude static maturity accounts
        if inputs['composite']:
            df_pp_max_sc['compStartDate'] = df_pp_max_sc['startDate'] + datetime.timedelta(30)
            df_pp_max_sc = df_pp_max_sc.loc[df_pp_max_sc['strategyControl'] == 'Enable']
            df_pp_max_sc = df_pp_max_sc.loc[df_pp_max_sc['date'].dt.date >= df_pp_max_sc['compStartDate']]
            df_pp_max_sc = df_pp_max_sc.loc[df_pp_max_sc['staticMaturity'] != 'Yes']
            df_pp_max_sc = df_pp_max_sc.loc[df_pp_max_sc['quarantine'] != 'Yes']
            # if MII, filter only on SPY
            if inputs['strategy'] == 'MII':
                df_pp_max_sc = df_pp_max_sc[(df_pp_max_sc['allowedTickers'].str.contains('SPY')) | \
                                            (df_pp_max_sc['basisRiskTicker'].str.contains('SPY')) | \
                                            (df_pp_max_sc['basisRiskTicker'].str.contains('XSP')) | \
                                            (df_pp_max_sc['basisRiskTicker'].str.contains('SPX'))]

    else:
        # if strategies aggregated, need to set placeholders for strategy config variables
        df_pp_max_sc = df_pp_all.assign(**{'strategyFocus':"", 'cashMandate':0})

    # if strategy focus is specified, filter data on focus
    if inputs['strategyFocus'] is not None:
        df_pp_max_sc = df_pp_max_sc[df_pp_max_sc['strategyFocus'] == inputs['strategyFocus']]

    # setting up stock values and filters
    df_pp_max_sc['secKey_cp'] = np.where(df_pp_max_sc['secKey_cp'] != 'None', 'Opt', 'None')
    df_pp_max_sc['sodStock'] = np.where(df_pp_max_sc['secKey_cp'] == 'None', df_pp_max_sc['pos'] * df_pp_max_sc['prevEodMark'], 0)
    # eodStock uses SOD strategy position because strategy config is SOD. This would cause error if we trade equity in an accnt with no allowedTickersCsc
    df_pp_max_sc['eodStock'] = np.where(df_pp_max_sc['secKey_cp'] == 'None', df_pp_max_sc['pos'] * df_pp_max_sc['eodMark'], 0)
    df_pp_max_sc['sodOption'] = np.where(df_pp_max_sc['secKey_cp'] != 'None', df_pp_max_sc['pos'] * df_pp_max_sc['prevEodMark'] * df_pp_max_sc['underliersPerCn'], 0)
    df_pp_max_sc['eodOption'] = np.where(df_pp_max_sc['secKey_cp'] != 'None', df_pp_max_sc['eodPos'] * df_pp_max_sc['eodMark'] * df_pp_max_sc['underliersPerCn'], 0)

    # remove timezone using tz_localize
    spy['date'] = pd.to_datetime(spy.date).dt.tz_localize(None)
    df_pp_max_sc['date'] = pd.to_datetime(df_pp_max_sc['date']).dt.tz_localize(None)

    # for MII composite
    df_pp_max_sc = df_pp_max_sc.merge(spy, 'left', 'date')
    df_pp_max_sc['notionalSPY'] = np.where((df_pp_max_sc['secKey_cp'] != 'None') & (df_pp_max_sc['underlyingTicker'] == 'SPY'), \
                                            -df_pp_max_sc['pos'] * df_pp_max_sc['underliersPerCn'] * df_pp_max_sc['prevClose'], 0)
    df_pp_max_sc['notionalSPX'] = np.where((df_pp_max_sc['secKey_cp'] != 'None') & (df_pp_max_sc['underlyingTicker'] == 'SPX'), \
                                            -df_pp_max_sc['pos'] * df_pp_max_sc['underliersPerCn'] * df_pp_max_sc['prevClose'] * 10, 0)
    df_pp_max_sc['notionalXSP'] = np.where((df_pp_max_sc['secKey_cp'] != 'None') & (df_pp_max_sc['underlyingTicker'] == 'XSP'), \
                                            -df_pp_max_sc['pos'] * df_pp_max_sc['underliersPerCn'] * df_pp_max_sc['prevClose'], 0)


    # aggregate by date and call/put with an accnt-strategy-focus to calc stock/option P&L
    grp = df_pp_max_sc.groupby(['accnt', 'strategyLevel', 'strategy', 'strategyFocus', 'underlyingTicker', 'date', 'secKey_cp'])
    df_pp_sum = grp['sodStock', 'eodStock', 'sodOption', 'eodOption', 'opnPnl', 'dayPnl', 'divPnl', 'notionalSPY', 'notionalSPX', 'notionalXSP'].sum().reset_index()
    df_pp_sum['stockPnl'] = np.where(df_pp_sum['secKey_cp'] == 'None', df_pp_sum['opnPnl'] + df_pp_sum['dayPnl'], 0)
    df_pp_sum['optPnl'] = np.where(df_pp_sum['secKey_cp'] == 'Opt', df_pp_sum['opnPnl'] + df_pp_sum['dayPnl'], 0)
    df_pp_sum['totalPnl'] = df_pp_sum['stockPnl'] + df_pp_sum['optPnl'] + df_pp_sum['divPnl']

    # aggregate by date with an accnt-strategy-focus to calc daily mandates
    df_pp_date_hec = df_pp_sum[df_pp_sum['strategy'] == 'HEC'].groupby(['accnt', 'strategyLevel', 'strategy', 'strategyFocus', 'underlyingTicker', 'date']).sum().reset_index()
    df_pp_date_hec['cashMandate'] = df_pp_max_sc[df_pp_max_sc['strategy'] == 'HEC'].groupby(['accnt', 'strategyLevel', 'strategy', 'strategyFocus', 'underlyingTicker', 'date'])['cashMandate'].max().reset_index()['cashMandate']
    df_pp_date_hec[['stockMandate', 'eodStockMandate']] = df_pp_date_hec.groupby(['accnt', 'strategyLevel', 'strategy', 'strategyFocus', 'underlyingTicker', 'date'])['sodStock', 'eodStock'].transform(sum)
    df_pp_date_non_hec = df_pp_sum[df_pp_sum['strategy'] != 'HEC'].groupby(['accnt', 'strategyLevel', 'strategy', 'strategyFocus', 'date']).sum().reset_index()
    df_pp_date_non_hec['cashMandate'] = df_pp_max_sc[df_pp_max_sc['strategy'] != 'HEC'].groupby(['accnt', 'strategyLevel', 'strategy', 'strategyFocus', 'date'])['cashMandate'].max().reset_index()['cashMandate']
    df_pp_date_non_hec[['stockMandate', 'eodStockMandate']] = df_pp_date_non_hec.groupby(['accnt', 'strategyLevel', 'strategy', 'strategyFocus', 'date'])['sodStock', 'eodStock'].transform(sum)
    df_pp_date_non_hec['underlyingTicker'] = 'None'
    df_pp_date = pd.concat([df_pp_date_hec, df_pp_date_non_hec], sort = True)
    if not inputs['aggregateStrats']:
        cond_cm = df_pp_date['cashMandate'].ne(0)
        df_pp_date['startMandate'] = np.where(cond_cm, df_pp_date['cashMandate'], df_pp_date['stockMandate'])
        df_pp_date['endMandate'] = np.where(cond_cm, df_pp_date['cashMandate'], df_pp_date['eodStockMandate'])
    else:
        df_pp_date['startMandate'] = df_pp_date['sodStock'] + df_pp_date['sodOption']
        df_pp_date['endMandate'] = df_pp_date['eodStock'] + df_pp_date['eodOption']

    if inputs['composite'] and inputs['strategy'] == 'MII':
        df_pp_date['startMandate'] = df_pp_date['notionalSPY'] + df_pp_date['notionalSPX'] + df_pp_date['notionalXSP']

    # aggregate everything here at group level to set up for daily return calcs
    if inputs['aggregateStrats']:
        df_pp_date = df_pp_date.groupby(grpLevel + ['date']).sum().reset_index()
        df_pp_date['aggLevel'] = aggLevelNonHEC
    else:
        df_pp_date_hec = df_pp_date[df_pp_date['strategy'] == 'HEC'].groupby(grpLevelHEC + ['date']).sum().reset_index()
        df_pp_date_hec['aggLevel'] = aggLevelHEC
        df_pp_date_non_hec = df_pp_date[df_pp_date['strategy'] != 'HEC'].groupby(grpLevel + ['date']).sum().reset_index()
        df_pp_date_non_hec['aggLevel'] = aggLevelNonHEC
        df_pp_date = pd.concat([df_pp_date_hec, df_pp_date_non_hec], sort = True)

    if inputs['aggregateStrats']:
        df_pp_date = df_pp_date.assign(**{'strategy':"", 'strategyLevel':"", 'strategyFocus':"", \
                                          'allowedTickers':"", 'allowedTickersPct':"", 'disallowedTickers':"", 'basisRiskTicker':""})
    elif inputs['aggregateAccnts']:
        df_pp_date = df_pp_date.assign(**{'strategyLevel':"", 'allowedTickers':"", 'allowedTickersPct':"", 'disallowedTickers':"", 'basisRiskTicker':""})
    else:
        df_pp_date = df_pp_date.merge(df_pp_max_sc[['accnt', 'strategy', 'strategyLevel', 'date', 'strategyFocus', \
                                                    'allowedTickers', 'allowedTickersPct', 'disallowedTickers', 'basisRiskTicker']].drop_duplicates()\
                         , 'left', on = ['accnt', 'strategy', 'strategyLevel', 'date'])


    # remove timezone using tz.localize()
    df_sh_h['date'] = pd.to_datetime(df_sh_h.date).dt.tz_localize(None)
    df_bm_h['date'] = pd.to_datetime(df_bm_h.date).dt.tz_localize(None)

    # merging with shadow returns and benchmark returns
    df_pp_date = df_pp_date.merge(df_sh_h, 'left', on = 'date').merge(df_bm_h, 'left', on = 'date')

    def add_spy_treas_bm(dr):

        # adding in SPY P&L for cash mandates
        if dr['strategy'] in ['MII', 'HEP', 'SDP'] and dr['cashMandate'] > 0:
            dr['stockPnl'] = dr['SPY'] * dr['startMandate']
            dr['totalPnl'] = dr['stockPnl'] + dr['optPnl']
            dr['divPnl'] = float(0)

        # adding in basis risk returns
        if not inputs['aggregateStrats']:
            if dr['basisRiskTicker'] != '':
                lst_basis_tk = dr['basisRiskTicker'].replace('SPX','SPY').replace('XSP','SPY').replace('RUT','IWM').replace('NDX','QQQ').replace('.','-').split(';')[0]
                dr['basisPnl'] = dr[lst_basis_tk] * dr['startMandate']

            else:
                dr['basisPnl'] = float(0)

        else:
            dr['basisPnl'] = float(0)

        # adding in shadow P&Ls
        if inputs['shadow'] == 'BIL':
            dr['ratePnl'] = dr[inputs['shadow']] * dr['startMandate']
            dr['totalPnl'] = dr['ratePnl'] + dr['optPnl']
            dr['divPnl'] = float(0)

        else:
            dr['ratePnl'] = float(0)

            if inputs['shadow'] is not None:
                dr['stockPnl'] = dr[inputs['shadow']] * dr['startMandate']
                dr['totalPnl'] = dr['stockPnl'] + dr['optPnl']
                dr['divPnl'] = float(0)


        # adding in benchmark P&Ls
        if dr['strategy'] == 'SNR' and dr['strategyFocus'] in ['SNR2', 'SNR3', 'SNR4', 'SNR5', 'SNR6', 'SNR7']:
            lst_bm_tk = dr['allowedTickers'].replace('SPX','SPY').replace('XSP','SPY').replace('RUT','IWM').replace('NDX','QQQ').split(';')
            if dr['allowedTickersPct'] == '':
                lst_bm_pct = dr['allowedTickersPct'].replace('','1').split(';')
            else:
                lst_bm_pct = dr['allowedTickersPct'].split(';')
            df = pd.Series(lst_bm_pct, index = lst_bm_tk).transpose().astype(float)
            ret = df.multiply(dr[lst_bm_tk])
            dr['bmPnl'] = ret.sum() * dr['startMandate']
            dr['bmTicker'] = ';'.join(np.unique(lst_bm_tk))

        elif dr['strategy'] == 'EFR':
            lst_bm_tk = dr['basisRiskTicker'].replace('SPX','SPY').replace('XSP','SPY').replace('RUT','IWM').replace('NDX','QQQ').split(';')[0]
            dr['bmPnl'] = dr[lst_bm_tk] * dr['startMandate']
            dr['bmTicker'] = ';'.join(np.unique(lst_bm_tk))

        else:
            dr['bmPnl'] = 0
            dr['bmTicker'] = ''

        if inputs['benchmark'] is not None:
            dr['bmPnl'] = dr[inputs['benchmark']] * dr['startMandate']
            dr['bmTicker'] = inputs['benchmark']

        return dr

    df_pp_date = df_pp_date.apply(add_spy_treas_bm, axis = 1)

    # fee return calculation (currently only IPI/MII composites)
    if inputs['fees']:
        if inputs['aggregateAccnts']:
            if inputs['strategy'] == 'CSP':
                annFee = 0.005
            elif inputs['strategy'] == 'MII':
                annFee = 0.006
    else:
        annFee = 0

    df_pp_date['stockReturn'] = df_pp_date['stockPnl'] / df_pp_date['startMandate']
    df_pp_date['divReturn'] = df_pp_date['divPnl'] / df_pp_date['startMandate']
    df_pp_date['stkDivReturn'] = df_pp_date['stockReturn'] + df_pp_date['divReturn']
    df_pp_date['rateReturn'] = df_pp_date['ratePnl'] / df_pp_date['startMandate']
    df_pp_date['optReturn'] = df_pp_date['optPnl'] / df_pp_date['startMandate']
    df_pp_date['feeReturn'] = annFee / 252
    df_pp_date['totalReturn'] = df_pp_date['totalPnl'] / df_pp_date['startMandate'] - df_pp_date['feeReturn']
    df_pp_date['basisReturn'] = df_pp_date['basisPnl'] / df_pp_date['startMandate']
    df_pp_date['bmReturn'] = df_pp_date['bmPnl'] / df_pp_date['startMandate']
    df_pp_date['diffReturn'] = df_pp_date['stkDivReturn'] - df_pp_date['basisReturn']
    df_pp_date = df_pp_date.replace([np.inf, -np.inf, np.nan], 0)

    df_pp_date['SR1'] = df_pp_date['stockReturn'] + 1
    df_pp_date['DR1'] = df_pp_date['divReturn'] + 1
    df_pp_date['SDR1'] = df_pp_date['stkDivReturn'] + 1
    df_pp_date['RR1'] = df_pp_date['rateReturn'] + 1
    df_pp_date['OR1'] = df_pp_date['optReturn'] + 1
    df_pp_date['TR1'] = df_pp_date['totalReturn'] + 1
    df_pp_date['BR1'] = df_pp_date['basisReturn'] + 1
    df_pp_date['BMR1'] = df_pp_date['bmReturn'] + 1

    # below needs to be done for each observation period (QTD, YTD, since Inception)
    dt_endDate = datetime.datetime.strptime(inputs['endDate'], '%Y-%m-%d')
    dict_IYQ = {'day'      : df_pp_date['date'].max(),
               'MTD'       : datetime.datetime(dt_endDate.year, dt_endDate.month, 1),
               'QTD'       : datetime.datetime(dt_endDate.year, ((dt_endDate.month - 1) // 3 + 1) * 3 - 2, 1),
               'YTD'       : datetime.datetime(dt_endDate.year, 1, 1),
               'Inception' : datetime.datetime.strptime(inputs['startDate'], '%Y-%m-%d')}

    df_report = pd.DataFrame()
    for key in dict_IYQ:

        df_pp_IYQ = df_pp_date[df_pp_date['date'] >= dict_IYQ[key]]
        if df_pp_IYQ.empty:
            logging.info('No data for period %s.' % key)
            continue

        strat_grp = df_pp_IYQ.groupby(grpLevel)
        df_pp_IYQ['daysCount'] = strat_grp['date'].cumcount() + 1
        df_pp_IYQ[['SRtr', 'DRtr', 'SDRtr', 'RRtr', 'ORtr', 'TRtr', 'BRtr', 'BMRtr']] = strat_grp[['SR1', 'DR1', 'SDR1', 'RR1', 'OR1', 'TR1', 'BR1', 'BMR1']].cumprod() - 1
        df_pp_IYQ[['annSRtr', 'annDRtr', 'annSDRtr', 'annRRtr', 'annORtr', 'annTRtr', 'annBRtr', 'annBMRtr']] = \
            (1 + df_pp_IYQ[['SRtr', 'DRtr', 'SDRtr', 'RRtr', 'ORtr', 'TRtr', 'BRtr', 'BMRtr']]).apply(lambda x: x ** (252 / df_pp_IYQ['daysCount']) - 1)
        # adjusting option return to tie out with total return
        df_pp_IYQ['ORtr'] = df_pp_IYQ['TRtr'] - df_pp_IYQ['SRtr'] - df_pp_IYQ['DRtr'] - df_pp_IYQ['RRtr']
        df_pp_IYQ['annORtr'] = df_pp_IYQ['annTRtr'] - df_pp_IYQ['annSRtr'] - df_pp_IYQ['annDRtr'] - df_pp_IYQ['annRRtr']

        if key == 'Inception':
            df_inception = df_pp_IYQ[grpLevel + ['date', 'SDRtr', 'TRtr', 'BRtr', 'BMRtr']]
            df_inception['_date'] = df_inception['date'].dt.strftime('%Y-%m-%dT00:00:00')

        df_pnl = strat_grp.sum().reset_index()[grpLevel + ['stockPnl', 'ratePnl', 'optPnl', 'divPnl', 'totalPnl', 'bmPnl']]
        df_sd = strat_grp.std().reset_index()[grpLevel + ['stkDivReturn', 'totalReturn', 'bmReturn']]
        df_sd[['stockVol', 'totalVol', 'bmVol']] = df_sd[['stkDivReturn', 'totalReturn', 'bmReturn']] * 252**(0.5)
        #df_sd = df_sd.rename(columns = {'stkDivReturn' : 'stockVol', 'totalReturn' : 'totalVol'})
        df_max_date = df_pp_IYQ[df_pp_IYQ['date'] == strat_grp['date'].transform(max)]\
            [grpLevel + ['date', 'aggLevel', 'endMandate', 'SRtr', 'DRtr', 'SDRtr', 'RRtr', 'ORtr', 'TRtr', 'BRtr', 'BMRtr', 'annSRtr', 'annDRtr', 'annSDRtr', 'annRRtr', 'annORtr', 'annTRtr', 'annBRtr', 'annBMRtr']]
        df_max_date = df_max_date.rename(columns = {'date' : 'endDate'})
        df_min_date = df_pp_IYQ[df_pp_IYQ['date'] == strat_grp['date'].transform(min)][grpLevel + ['date', 'startMandate']]
        df_min_date = df_min_date.rename(columns = {'date' : 'startDate'})

        df_stats = df_min_date.merge(df_max_date, 'left', grpLevel)\
                               .merge(df_pnl, 'left', grpLevel)\
                               .merge(df_sd, 'left', grpLevel)

        if not inputs['aggregateAccnts']:
            df_stats = df_stats.merge(df_ac[['accnt', 'custodianAccnt', 'lastName', 'institutionalAdvisor', 'branchAdvisor', 'financialAdvisor']], 'left', 'accnt')
            if not inputs['aggregateStrats']:
                df_stats = df_stats.merge(df_sc_hist_max[['accnt', 'strategyLevel', 'strategy', 'strategyFocus', 'allowedTickers', 'basisRiskTicker']], 'left', \
                                          on = ['accnt', 'strategyLevel', 'strategy'])
                if not inputs['aggregateTickers']:
                    df_stats['allowedTickers'] = np.where(df_stats['aggLevel'] == 'Ticker', df_stats['underlyingTicker'], df_stats['allowedTickers'])

        df_stats['period'] = key
        df_stats['volRed'] = np.where(df_stats['stockVol'] == 0, 0, -(df_stats['totalVol'] / df_stats['stockVol'] - 1))
        df_stats['stockSR'] = df_stats['annSDRtr'] / df_stats['stockVol']
        df_stats['totalSR'] = df_stats['annTRtr'] / df_stats['totalVol']
        df_stats['bmSR'] = df_stats['annBMRtr'] / df_stats['bmVol']

        df_report = df_report.append(df_stats)

    df_report[['startMandate', 'endMandate', 'stockPnl', 'ratePnl', 'optPnl', 'divPnl', 'totalPnl']] = df_report[['startMandate', 'endMandate', 'stockPnl', 'ratePnl', 'optPnl', 'divPnl', 'totalPnl']].round(0)
    df_report[['SRtr', 'DRtr', 'SDRtr', 'RRtr', 'ORtr', 'TRtr', 'BRtr', 'BMRtr', 'annSRtr', 'annDRtr', 'annSDRtr', 'annRRtr', 'annORtr', 'annTRtr', 'annBRtr', 'annBMRtr', 'stockVol', 'totalVol', 'bmVol', 'volRed']] = (df_report\
             [['SRtr', 'DRtr', 'SDRtr', 'RRtr', 'ORtr', 'TRtr', 'BRtr', 'BMRtr', 'annSRtr', 'annDRtr', 'annSDRtr', 'annRRtr', 'annORtr', 'annTRtr', 'annBRtr', 'annBMRtr', 'stockVol', 'totalVol', 'bmVol', 'volRed']] * 100).round(2)

    if not inputs['aggregateTickers']:
        df_pp_date['allowedTickers'] = np.where(df_pp_date['aggLevel'] == 'Ticker', df_pp_date['underlyingTicker'], df_pp_date['allowedTickers'])

    if inputs['aggregateAccnts']:
        df_pp_date['accnt'] = ""
        df_pp_date['strategyLevel'] = ""
        df_report['institutionalAdvisor'] = df_report['strategy']
        df_report['branchAdvisor'] = ""
        df_report['financialAdvisor'] = ""
        df_report['custodianAccnt'] = df_report['strategyFocus']
        df_report['accnt'] = ""
        df_report['lastName'] = ""
        df_report['strategyLevel'] = ""
    else:
        df_report['custodianAccnt'] = 'xxxx' + df_report['custodianAccnt'].str[-4:]
        if inputs['aggregateStrats']:
            df_report['strategy'] = "Total"
            df_report['strategyLevel'] = ""

    df_report = df_report.apply(lambda x: null_per_strategy(x), axis = 1)
    df_report.replace({np.nan : None}, inplace=True)


    OUTPUT_DIR = setOutputFilePath(OUTPUT_SUBDIRECTORY = os.path.join('perf_report', datetime.datetime.today().strftime('%Y-%m-%d')), OUTPUT_FILE_NAME = '')
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    if inputs['output'] == 'monthly':
        df_monthlies = df_pp_date.copy()
        df_monthlies['month'] = df_monthlies['date'].dt.month
        df_monthlies['year'] = df_monthlies['date'].dt.year
        df_mo_sum = df_monthlies.groupby(grpLevel + ['year', 'month'])[['stockPnl', 'divPnl', 'ratePnl', 'optPnl', 'totalPnl']].sum().reset_index()
        df_mo_prod = df_monthlies.groupby(grpLevel + ['year', 'month'])[['SR1', 'DR1', 'SDR1', 'RR1', 'OR1', 'TR1', 'BMR1', 'BR1']].prod().reset_index()
        df_mo_prod[['stockReturn', 'divReturn', 'stkDivReturn', 'rateReturn', 'optReturn', 'totalReturn', 'bmReturn', 'basisReturn']] = \
                df_mo_prod[['SR1', 'DR1', 'SDR1', 'RR1', 'OR1', 'TR1', 'BMR1', 'BR1']] - 1
        df_mo_prod['diffReturn'] = df_mo_prod['stkDivReturn'] - df_mo_prod['basisReturn']
        df_mo = df_mo_sum.merge(df_mo_prod, 'inner', grpLevel + ['year', 'month'])
        df_mo['date'] = df_mo.apply(lambda x: datetime.datetime(x['year'], x['month'], 1), axis = 1)
        df_mo[['aggLevel', 'accnt', 'strategyLevel', 'strategy', 'strategyFocus', 'allowedTickers', 'disallowedTickers', 'basisRiskTicker', 'bmTicker', 'startMandate']] = \
                df_monthlies.loc[df_monthlies.groupby(grpLevel + ['year', 'month'])['date'].idxmin()].reset_index()\
                [['aggLevel', 'accnt', 'strategyLevel', 'strategy', 'strategyFocus', 'allowedTickers', 'disallowedTickers', 'basisRiskTicker', 'bmTicker', 'startMandate']]
        df_mo['endMandate'] = df_monthlies.loc[df_monthlies.groupby(grpLevel + ['year', 'month'])['date'].idxmax()].reset_index()['endMandate']
        if generate_json:
            if inputs['aggregateAccnts']:
                file_name = '%s.csv' % np.unique(df_mo['strategy'].tolist())
            else:
                file_name = '%s.csv' % inputs['output']
            df_mo.to_csv(os.path.join(OUTPUT_DIR, file_name))

    elif inputs['output'] == 'daily':
        df_dailies = df_pp_date.copy()
        if generate_json:
            if inputs['aggregateAccnts']:
                file_name = '%s.csv' % np.unique(df_dailies['strategy'].tolist())
            else:
                file_name = '%s.csv' % inputs['output']
            df_dailies.to_csv(os.path.join(OUTPUT_DIR, file_name))


    if generate_json:

        df_report_list = df_pp_date[grpLevel].drop_duplicates()
        r, c = df_report_list.shape
        logging.info(f"Report count: {r}")

        for index, row in df_report_list.iterrows():

            df_accnt_strat_qtd = filter_df_period(df_report, row, grpLevel, True, 'QTD')
            df_accnt_strat_ytd = filter_df_period(df_report, row, grpLevel, True, 'YTD')
            df_accnt_strat_inception = filter_df_period(df_report, row, grpLevel, True, 'Inception')

            if df_accnt_strat_qtd is None:
                logging.info(f"No QTD data for: {row}.")
                continue

            else:
                df_dailies = filter_df_period(df_inception, row, grpLevel, False)
                lst_stock = df_dailies[['_date', 'SDRtr']].rename(columns = {'SDRtr' : '_value'}).to_dict('records')
                lst_total = df_dailies[['_date', 'TRtr']].rename(columns = {'TRtr' : '_value'}).to_dict('records')
                lst_bm = df_dailies[['_date', 'BMRtr']].rename(columns = {'BMRtr' : '_value'}).to_dict('records')
                include_stock = df_accnt_strat_inception['strategy'] not in ['CSP','PUT','SNR']
                include_bm = (df_accnt_strat_inception['strategy'] == 'SNR' and \
                              df_accnt_strat_inception['strategyFocus'] in ['SNR2', 'SNR3', 'SNR4', 'SNR5', 'SNR6', 'SNR7']) or\
                             (df_accnt_strat_inception['strategy'] == 'EFR') or \
                             (inputs['benchmark'] is not None)
                if not include_bm:
                    bmTickers = None
                else:
                    if inputs['benchmark'] is not None:
                        bmTickers = inputs['benchmark']
                    elif df_accnt_strat_inception['strategy'] == 'SNR':
                        bmTickers = df_accnt_strat_qtd['allowedTickers'].replace('SPX','SPY').replace('XSP','SPY').replace('RUT','IWM').replace('NDX','QQQ')
                    elif df_accnt_strat_inception['strategy'] == 'EFR':
                        bmTickers = df_accnt_strat_qtd['basisRiskTicker'].replace('SPX','SPY').replace('XSP','SPY').replace('RUT','IWM').replace('NDX','QQQ').split(';')[0]

                if (df_accnt_strat_qtd['endDate'] - df_accnt_strat_inception['startDate']).days > 365:
                    ret_pre = 'ann'
                else:
                    ret_pre = ''

                dict_output = {
                              "_reportItems": [
                              dict_aum_ret_rsk(df_accnt_strat_qtd, 'QTD'),
                              dict_aum_ret_rsk(df_accnt_strat_ytd, 'YTD'),
                              dict_aum_ret_rsk(df_accnt_strat_inception, 'INCEPTION', ret_pre)
                              ],
                              "_returnVsRiskGraph": {
                                  "_title": "RETURN vs RISK",
                                  "_period": "Since Inception",
                                  "_labelA": df_accnt_strat_inception['strategy'],
                                  "_labelB": "Ticker(s)",
                                  "_labelC": "Benchmark",
                                  "_equityReturnPct": df_accnt_strat_inception['%sSDRtr' % ret_pre] if include_stock else None,
                                  "_totalReturnPct": df_accnt_strat_inception['%sTRtr' % ret_pre],
                                  "_benchmarkReturnPct": df_accnt_strat_inception['%sBMRtr' % ret_pre] if include_bm else None,
                                  "_equityVolatility": df_accnt_strat_inception['stockVol'] if include_stock else None,
                                  "_totalVolatility": df_accnt_strat_inception['totalVol'],
                                  "_benchmarkVolatility": df_accnt_strat_inception['bmVol'] if include_bm else None
                              },
                              "_trailingPerformanceGraph": {
                                  "_title": "TRAILING PERFORMANCE",
                                  "_period": "Since Inception",
                                  "_labelA": df_accnt_strat_inception['strategy'],
                                  "_labelB": "Ticker(s)" if include_stock else None,
                                  "_labelC": "Benchmark" if include_bm else None,
                                  "_lineA": lst_total,
                                  "_lineB": lst_stock if include_stock else None,
                                  "_lineC": lst_bm if include_bm else None
                              },
                              "_custodianAccount": df_accnt_strat_qtd['custodianAccnt'] if not inputs['aggregateAccnts'] else "",
                              "_inceptionDate": df_accnt_strat_inception['startDate'].strftime('%b %d, %Y'),
                              "_reportPeriod": "",
                              "_reportEndDate": df_accnt_strat_qtd['endDate'].strftime('%m/%d/%Y'),
                              "_strategy": df_accnt_strat_qtd['strategy'],
                              "_includedTickers": df_accnt_strat_qtd['allowedTickers'] if (not inputs['aggregateAccnts'] and not inputs['aggregateStrats']) else "",
                              "_benchmarkTicker": bmTickers if include_bm else "",
                              "_equityLabel": "Equity"
                              }

                ADV_OUTPUT_DIR = setOutputFilePath(OUTPUT_SUBDIRECTORY = os.path.join('perf_report', datetime.datetime.today().strftime('%Y-%m-%d'), \
                                                                                  df_accnt_strat_qtd['institutionalAdvisor']), OUTPUT_FILE_NAME = '')
                if not os.path.exists(ADV_OUTPUT_DIR):
                    os.makedirs(ADV_OUTPUT_DIR)
                file_name_json = os.path.join(ADV_OUTPUT_DIR, '%s%s%s.%s.%s.%s.%s%s-%s.json' % (df_accnt_strat_qtd['institutionalAdvisor'],
                                                                                ('.%s' % df_accnt_strat_qtd['branchAdvisor']) if inputs['advisorDetail'] else '',
                                                                                ('.%s' % df_accnt_strat_qtd['financialAdvisor']) if inputs['advisorDetail'] else '',
                                                                                df_accnt_strat_qtd['custodianAccnt'],
                                                                                df_accnt_strat_qtd['lastName'],
                                                                                df_accnt_strat_qtd['strategy'],
                                                                                df_accnt_strat_qtd['strategyLevel'][0:3],
                                                                                ('.%s' % df_accnt_strat_qtd['allowedTickers']) if df_accnt_strat_qtd['aggLevel'] == 'Ticker' else '',
                                                                                df_accnt_strat_qtd['accnt']))

                with open(file_name_json, 'w') as open_file:
                    open_file.write(json.dumps(dict_output, sort_keys=False, indent=4, separators=(',', ':')))

                download_all_files(file_name_json)
                logging.info(f"JSON file created: {file_name_json}")
    else:
        if freq == 'monthly':
            return df_mo
        else:
            return df_pp_date


# --------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------
# run main

if __name__ == '__main__':

    setLogging(LOGGING_DIRECTORY = None, LOG_FILE_NAME = os.path.basename(__file__), level = 'DEBUG')

    logging.info('Process ID: %s' % os.getpid())
    logging.debug('Running Script: %s' % __file__)

    try:
        run_performance(inputs, True)
    except Exception as e:
        logging.error(e, exc_info=True)

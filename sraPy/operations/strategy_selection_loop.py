# -*- coding: utf-8 -*-
"""
Created on Mon Mar 16 17:58:54 2020

@author: mark.lutz
"""

import pandas as pd
import numpy as np
import datetime


def strategy_rules_cpls(strategy, stratFocus, okey_cp, pos):

    # for a given strategy and strategy focus,
    # determines whether the long/short call/put/stock is allowed

    if (strategy in ['HEC', 'MII', 'OYE', 'OYENT', 'HEP', 'SDP', 'EFR', 'BXM', 'ISR', 'CMG', 'TAIL']) & \
       (okey_cp == 'None'):
        return True

    elif (stratFocus == 'Custom') & (okey_cp != 'None'):
        return True

    elif strategy == 'HEC':
        if stratFocus == 'CallOnly':
            if okey_cp == 'Call':
                return True
            else:
                return False
        elif stratFocus in ['PutOnly', 'PutSpread']:
            if okey_cp == 'Put':
                return True
            else:
                return False
        elif stratFocus in ['Collar', 'PSC', 'PSC50', 'PSC100']:
            if (okey_cp == 'Put') | \
               (okey_cp == 'Call'):
                return True
            else:
                return False
        else:
            return False

    elif strategy in ['MII', 'OYE', 'OYENT', 'BXM']:
        if okey_cp == 'Call':
            return True
        else:
            return False

    elif strategy == 'HEP':
        if stratFocus in ['PutOnly', 'PutSpread', 'SDP']:
            if okey_cp == 'Put':
                return True
            else:
                return False
        elif stratFocus in ['Default', 'Collar', 'PSC', 'PSC50', 'PSC100']:
            if (okey_cp == 'Put') | \
               (okey_cp == 'Call'):
                return True
            else:
                return False
        else:
            return False

    elif strategy in ['CSP', 'SDP', 'PUT']:
        if okey_cp == 'Put':
            return True
        else:
            return False

    elif strategy in ['NDE', 'ISR']:
        if stratFocus == 'CallOnly':
            if okey_cp == 'Call':
                return True
            else:
                return False
        elif stratFocus == 'PutOnly':
            if okey_cp == 'Put':
                return True
            else:
                return False
        elif stratFocus == 'Default':
            if (okey_cp == 'Put') | \
               (okey_cp == 'Call'):
                return True
            else:
                return False
        elif stratFocus == 'NDE':
            if (okey_cp == 'Put') | \
               (okey_cp == 'Call'):
                return True
            else:
                return False
        elif stratFocus == 'ASM':
            if (okey_cp == 'Put') | \
               (okey_cp == 'Call'):
                return True
            else:
                return False
        else:
            return False

    elif (strategy in ['ASM', 'SNR', 'EFR', 'TAIL']) & \
         (okey_cp != 'None'):
        return True

    else:
        return False


def assign_strategy(i, r, dr_accnt_security_key, di, df_accnt_security_key, df_complex):

    # tag an accnt-position combination based on the strategy config provided
    # both stock positions and option positions can be inputs
    # adds a column to the dataframe/series provided called pos_strat

    lst_allowed_tickers = r['allowedTickers'].replace(',', ';').split(';')
    lst_disallowed_tickers = r['disallowedTickers'].replace(',', ';').split(';')
    lst_basis_risk_tickers = r['basisRiskTicker'].replace(',', ';').split(';')

    def add_strategy_to_df(r, dr_accnt_security_key, di, df_accnt_security_key):
        if ('pos_strat' in dr_accnt_security_key.index) and (dr_accnt_security_key['okey_cp'] == 'None'):
            df_accnt_security_key.loc[df_accnt_security_key.index.max()+1] = dr_accnt_security_key
        df_accnt_security_key.at[di,'pos_strat_lvl'] = r['strategyLevel']
        df_accnt_security_key.at[di,'pos_strat'] = r['strategy']
        dr_accnt_security_key['pos_strat_lvl'] = r['strategyLevel']
        dr_accnt_security_key['pos_strat'] = r['strategy']

    if df_complex is not None:
        srs_cplx_ask = df_complex.merge(dr_accnt_security_key.to_frame().transpose(), 'inner', on = 'ticker_tk').squeeze()
        if srs_cplx_ask.empty:
            lst_ticker = [dr_accnt_security_key['ticker_tk']]
        else:
            lst_ticker = [srs_cplx_ask['underlyingTicker1'], srs_cplx_ask['underlyingTicker2'], srs_cplx_ask['underlyingTicker3']]
    else:
        lst_ticker = [dr_accnt_security_key['ticker_tk']]

    if 'date' in dr_accnt_security_key:
        if dr_accnt_security_key['date'] == r['endDate']:
            closeDateOverride = True
        else:
            closeDateOverride = False
    else:
        closeDateOverride = False

    for tk in lst_ticker:
        tkIndex = tk.replace('SPX','SPY').replace('RUT','IWM').replace('NDX','QQQ')
        if (tk != '_CASH') & \
           (tk != '') & \
           ((r['strategyControl'] != 'Close') | closeDateOverride) & \
           (r['startDate'] > datetime.datetime(1900,1,1)) & \
           strategy_rules_cpls(r['strategy'], r['strategyFocus'], dr_accnt_security_key['okey_cp'], dr_accnt_security_key['pos']):
    
            if ((tk in lst_allowed_tickers) | (tkIndex in lst_allowed_tickers)) & \
               (
               (r['strategy'] in ('HEC', 'MII', 'OYE', 'OYENT', 'HEP', 'SDP', 'EFR', 'BXM', 'ISR', 'TAIL')) | \
               (dr_accnt_security_key['okey_cp'] != 'None')
               ):
    
                add_strategy_to_df(r, dr_accnt_security_key, di, df_accnt_security_key)
                return df_accnt_security_key
    
            elif ((tk in lst_basis_risk_tickers) | (tkIndex in lst_basis_risk_tickers)) & \
               (r['strategy'] in ('HEC', 'MII', 'OYE', 'HEP', 'SDP', 'EFR', 'BXM', 'ISR', 'TAIL')) & \
               (dr_accnt_security_key['okey_cp'] != 'None'):
    
                add_strategy_to_df(r, dr_accnt_security_key, di, df_accnt_security_key)
                return df_accnt_security_key
    
            elif (tk not in lst_disallowed_tickers) & \
               (r['strategy'] in ('MII', 'OYE', 'OYENT', 'HEP', 'SDP', 'BXM')) & \
               (r['cashMandate'] == 0) & \
               (dr_accnt_security_key['ticker_at'] != 'BND') & \
               (len(dr_accnt_security_key['ticker_tk']) <= 5) & \
               (dr_accnt_security_key['okey_cp'] == 'None') & \
               (r['allowedTickers'] == ''):
    
                add_strategy_to_df(r, dr_accnt_security_key, di, df_accnt_security_key)
                return df_accnt_security_key
    
            # OYE, OYENT can have options without any of the above
    
            elif (tk not in lst_disallowed_tickers) & \
               (
               (r['strategy'] in ('OYE', 'OYENT')) | \
               ((r['strategy'] == 'CSP') & (r['strategyFocus'] == 'CSPAddon'))
               ) & \
               (dr_accnt_security_key['okey_cp'] != 'None') & \
               (r['allowedTickers'] == ''):
    
                add_strategy_to_df(r, dr_accnt_security_key, di, df_accnt_security_key)
                return df_accnt_security_key
    
            elif (r['strategy'] == 'CMG') & \
               (dr_accnt_security_key['ticker_at'] == 'BND') & \
               (tk[:3] == '912'):
    
                add_strategy_to_df(r, dr_accnt_security_key, di, df_accnt_security_key)
                return df_accnt_security_key

        elif (tk == '_CASH') & \
           ((r['strategyControl'] != 'Close') | closeDateOverride) & \
           (r['startDate'] > datetime.datetime(1900,1,1)):

            if r['strategy'] in ('CSP', 'SNR', 'CMG', 'PUT'):

                add_strategy_to_df(r, dr_accnt_security_key, di, df_accnt_security_key)
                return df_accnt_security_key

    return df_accnt_security_key


def pos_strategy_selection(di, dr_accnt_security_key, df_accnt_security_key, df_strategy_config, df_complex):

    # iterate over strategies per accnt to determine which (if any) should be tied to the position
    # returns the series provided (or each row of the dataframe provided)

    accnt = dr_accnt_security_key['accnt']

    if 'okey_cp' not in dr_accnt_security_key.index:
        dr_accnt_security_key['okey_cp'] = 'None'

    for i, r in df_strategy_config[df_strategy_config['accnt'] == accnt].iterrows():
        df_accnt_security_key = assign_strategy(i, r, dr_accnt_security_key, di, df_accnt_security_key, df_complex)

    if 'pos_strat' not in dr_accnt_security_key.index:
        df_accnt_security_key.at[di,'pos_strat_lvl'] = 'None'
        df_accnt_security_key.at[di,'pos_strat'] = 'None'




def apply_pos_strategy_selection(df_accnt_security_key, df_strategy_config):

    return df_accnt_security_key.apply(lambda x: pos_strategy_selection(x, df_strategy_config), axis = 1)


def strategy_selection(df_strategy_config,
                       df_accnt_security_key = None,
                       srs_accnt_security_key = None,
                       accnt = None,
                       ticker_at = None,
                       ticker_tk = None,
                       okey_cp = None,
                       pos = None,
                       df_complex = None):

    if df_accnt_security_key is not None:

        for di, dr in df_accnt_security_key.iterrows():
            pos_strategy_selection(di, dr, df_accnt_security_key, df_strategy_config, df_complex)

    elif srs_accnt_security_key is not None:

        df_accnt_security_key = apply_pos_strategy_selection(srs_accnt_security_key, df_strategy_config)

    elif accnt is not None and ticker_at is not None and ticker_tk is not None and pos is not None:

        dict_accnt_security_key = {'accnt':accnt, 'ticker_at':ticker_at, 'ticker_tk':ticker_tk, 'okey_cp':okey_cp, 'pos':pos}
        srs_accnt_security_key = pd.Series(dict_accnt_security_key)
        pos_strategy_selection(0, srs_accnt_security_key, df_strategy_config)
        df_accnt_security_key = srs_accnt_security_key

    else:

        print('Adequate account-security data not provided')
        return

    return df_accnt_security_key


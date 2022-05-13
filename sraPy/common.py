# --------------------------------------------------------------------------------------------------
# common.py
#
# This script contains useful common functions for python scripts.
# --------------------------------------------------------------------------------------------------
# created by joe.loss

import os, warnings
import pendulum
import numpy as np
import pandas as pd
from pathlib2 import Path
from datetime import timedelta, datetime as dt
from exchange_calendars.exchange_calendar_xnys import XNYSExchangeCalendar as nyseCal

warnings.simplefilter('ignore', category=FutureWarning)


# --------------------------------------------------------------------------------------------------
# pandas settings

def setPandas(float_precision=2):

    options = {
        'display': {
            'max_columns': None,
            'max_colwidth': 800,
            'colheader_justify': 'center',
            'max_rows': 60,
            'min_rows': 10,
            'precision': 2,
            'float_format': f"{{:,.{float_precision}f}}".format,
            'expand_frame_repr': False,  # Don't wrap to multiple pages
        },
        'mode': {
            'chained_assignment': None   # Controls SettingWithCopyWarning
        }
    }
    for category, option in options.items():
        for op, value in option.items():
            pd.set_option(f'{category}.{op}', value)


# --------------------------------------------------------------------------------------------------
# fn: Create Option Keys

def fnCreateOptKeys(df, key = 'secKey', has_secType=True):

    optKey = df[[key + '_tk',
                       key + '_yr',
                       key + '_mn',
                       key + '_dy',
                       key + '_xx',
                       key + '_cp']].astype('str').agg(':'.join, axis = 1)

    # return optKey if secType = option, else return stkKey
    if has_secType:
        stkKey = df[key + '_tk'] + ':' + 'Stk'
        key = np.where(df.secType == 'Option', optKey, stkKey)
    else:
        key = optKey

    return key


# --------------------------------------------------------------------------------------------------
# set up output filepath

def setOutputFilePath(OUTPUT_DIRECTORY = os.path.join('/sradeveloper', 'tmp', 'advisorscodebase'), OUTPUT_SUBDIRECTORY = None, OUTPUT_FILE_NAME = None):
    if not OUTPUT_FILE_NAME:
        OUTPUT_FILE_NAME = ""
    else:
        OUTPUT_FILE_NAME = OUTPUT_FILE_NAME
    if OUTPUT_SUBDIRECTORY:
        if OUTPUT_SUBDIRECTORY.startswith('/'):
            OUTPUT_DIRECTORY = os.path.join(OUTPUT_DIRECTORY, '.' + OUTPUT_SUBDIRECTORY)
        else:
            OUTPUT_DIRECTORY = os.path.join(OUTPUT_DIRECTORY, './' + OUTPUT_SUBDIRECTORY)

        if not os.path.exists(OUTPUT_DIRECTORY):
            os.makedirs(OUTPUT_DIRECTORY)
    else:
        if OUTPUT_DIRECTORY.startswith('/'):
            if not os.path.exists(OUTPUT_DIRECTORY):
                os.makedirs(OUTPUT_DIRECTORY)
        else:
            OUTPUT_DIRECTORY = "/" + OUTPUT_DIRECTORY
            if not os.path.exists(OUTPUT_DIRECTORY):
                os.makedirs(OUTPUT_DIRECTORY)

    path = os.path.join('/', os.path.relpath(OUTPUT_DIRECTORY, ''), OUTPUT_FILE_NAME)
    if not Path(path).parent.exists():
        os.makedirs(Path(path).parent)

    return path


# --------------------------------------------------------------------------------------------------
# remove AM expiration contracts from the dataframe for that day

def fnRemoveAMSettlementContracts(df, lstTickers = ['RUT', 'NDX', 'SPX']):

    # extract dates from option contract
    df['expDate'] = df[['okey_yr', 'okey_mn', 'okey_dy']].astype('str').agg('-'.join, axis = 1)

    # if contract date == today's date, filter on that contract
    df['expDate'] = pd.to_datetime(df['expDate'])
    curDate = dt.today().date()

    cond1 = df['expDate'].dt.date == curDate
    cond2 = df['okey_tk'].isin(lstTickers)

    # current time
    curTime = dt.now()
    # time at midnight
    startTime = dt.combine(curDate, dt.min.time())
    # expiration time
    expTime = startTime + timedelta(hours = 10)

    # drop expiring contract if current time > 10AM on expiration and new contract exists
    if len(df) > 1:
        if curTime.time() > expTime.time():
            df = df.loc[~(cond1 & cond2)]
    else:
        pass
    return df


# --------------------------------------------------------------------------------------------------
# returns last trade date

def fnLastValidTradeDate(dttm=None):
    ecal = nyseCal()

    if not dttm:
        dttm = pendulum.today("America/Chicago").timestamp()
        ts = dttm
    else:
        if type(dttm) != float:
            if type(dttm) == str:
                ts = pendulum.parse(dttm, tz="America/Chicago").timestamp()
            elif type(dttm) == pendulum.Date:
                ts = pendulum.parse(dttm.to_date_string(), tz="America/Chicago").timestamp()
            else:
                ts = dttm
        else:
            ts = dttm

    curTimestamp = pd.Timestamp.fromtimestamp(ts)
    d = ecal.previous_close(dt=curTimestamp)
    prevDay = pendulum.from_timestamp(d.timestamp()).to_date_string()

    return prevDay


# --------------------------------------------------------------------------------------------------
# returns next trade date

def fnNextValidTradeDate(dttm=None):
    ecal = nyseCal()

    if not dttm:
        dttm = pendulum.today("America/Chicago").add(days=1).timestamp()
        ts = dttm
    else:
        if type(dttm) != float:
            if type(dttm) == str:
                ts = pendulum.parse(dttm, tz="America/Chicago").add(days=1).timestamp()
            elif type(dttm) == pendulum.Date:
                ts = pendulum.parse(dttm.to_date_string(), tz="America/Chicago").timestamp()
            else:
                ts = dttm
        else:
            ts = dttm

    curTimestamp = pd.Timestamp.fromtimestamp(ts)
    d = ecal.next_close(dt=curTimestamp)
    nextDay = pendulum.from_timestamp(d.timestamp()).to_date_string()

    return nextDay


# --------------------------------------------------------------------------------------------------
# fn to ignore future and deprecated warnings

def suppressWarnings(warningType= [FutureWarning, DeprecationWarning]):
    for category in warningType:
        print("Ignoring %s" % category)
        warnings.simplefilter(action="ignore", category=category)

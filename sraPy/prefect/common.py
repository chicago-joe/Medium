import prefect
from prefect import task
from sraPy.common import fnCreateOptKeys

import pandas as pd
from prefect import task
from dateutil.relativedelta import *
from dateutil.rrule import *
from exchange_calendars.exchange_calendar_xnys import XNYSExchangeCalendar as nyseCal
import pendulum


@task(name="Upload SQL Task")
def UploadSQLTask(controller=None, df=None, db=None, table=None, mode='REPLACE', colNames=None, unlinkFile=True):
    logger = prefect.context.get("logger")

    if not controller:
        logger.error('Please attach a MYSQL Controller Class')
        return
    if not unlinkFile:
        unlinkFile=True

    controller.upload(df, db, table, mode, colNames, unlinkFile)
    return


@task(name="Create Historical strategyFocus Mapping")
def create_historical_strategyFocus_mapping(controller=None, df=None, lstStrat=None, lstFocus=None, minDate=None, maxDate=None, optKeyType='secKey'):
    logger = prefect.context.get("logger")

    if not controller:
        logger.error('Please attach a MYSQL Controller Class')
        return

    if lstFocus:
        rs = lstFocus
    elif lstStrat:
        rs = lstStrat
    else:
        logger.warning('No strategies were selected! Loading possible defaults from beta_sheet_daily_strategy_risk_bf')
        df = controller.query(
            select=['DISTINCT strategy, strategyFocus'],
            db='sradb',
            table='beta_sheet_daily_strategy_risk_bf'
        )

        for col in ['strategy','strategyFocus']:
            df.loc[df[col]=="",col]=None

        lstStrat=df['strategy'].unique().tolist()
        lstFocus=df['strategyFocus'].unique().tolist()


    logger.info('Breaking out strategy focus historically...')

    if 'optKey' not in df.columns.unique().tolist():
        df['optKey'] = fnCreateOptKeys(df, optKeyType, has_secType = False)

    df.reset_index(drop=True, inplace=True)
    df['keyATD'] = df[['accnt', 'strategy', 'optKey', 'date']].astype('str').agg(':'.join, axis = 1)

    # save strategy info for later
    stratLevels = list(zip(df['keyATD'], df['strategyLevel']))

    # aggregate by accnt:optionkey
    df = df.groupby('keyATD').sum()
    df.reset_index(inplace = True)

    # parse key back into columns
    df['date'] = df['keyATD'].str.split(":", expand = True)[8]
    df['strategy'] = df['keyATD'].str.split(":", expand = True)[1]
    df['accnt'] = df['keyATD'].str.split(":", expand = True)[0]
    df['underlyingTicker'] = df['keyATD'].str.split(":", expand = True)[2]
    df['secKey_cp'] = df['keyATD'].str.split(":", expand = True)[7]
    df['secKey_xx'] = df['keyATD'].str.split(":", expand = True)[6]
    df['secKey_tk'] = df['underlyingTicker']

    strategyLevel = { }

    for row in enumerate(stratLevels):
        keys = row[1][0]
        values = row[1][1]
        strategyLevel.update({keys:values})

    # map info to keys
    df['strategyLevel'] = df['keyATD'].map(strategyLevel)

    dfResult = controller.get_strategyFocus(df, minDate, maxDate)
    dfResult['secKey_at'] = 'EQT'

    return dfResult


@task(name="Map Daily Mandates")
def map_daily_mandates(controller=None, df=None, minDate=None, maxDate=None):
    logger = prefect.context.get("logger")

    if not controller:
        logger.error('Please attach a MYSQL Controller Class')
        return

    logger.info('Mapping daily mandates to accnt-strategy-strategyLevel...')
    dfResult = controller.map_daily_mandates(df, minDate, maxDate)
    return dfResult


# --------------------------------------------------------------------------------------------------
# get list of expiration fridays for the year

@task()
def get_monthly_expirations(startDate=None, endDate=None):
    logger = prefect.context.get('logger')
    lstMonthlyExpirations=[]

    if not startDate:
        startDate = pendulum.today("America/Chicago").start_of("year").to_date_string()
    if not endDate:
        endDate = pendulum.today("America/Chicago").end_of("year").to_date_string()

    logger.info(f'loading option expiration dates for period {startDate} - {endDate}')
    ecal = nyseCal(startDate, endDate)

    date_range = pendulum.period(pendulum.parse(startDate), pendulum.parse(endDate))
    months = date_range.in_months()
    lstThirdFriday = list(rrule(MONTHLY, count = months, byweekday = FR(3), dtstart = pendulum.parse(startDate).date()))

    for date in lstThirdFriday:
        validContractExpiry = ecal.date_to_session_label(date=date, direction = 'previous').strftime("%Y-%m-%d")
        lstMonthlyExpirations.append(validContractExpiry)

    # sort months
    expirationDates = sorted(list(set(lstMonthlyExpirations)))

    return expirationDates


# --------------------------------------------------------------------------------------------------
# get list of expiration fridays for the year

@task()
def get_trading_dates(startDate=None, endDate=None):
    logger = prefect.context.get('logger')

    if not startDate:
        startDate = pendulum.date(1900,1,1)
    else:
        startDate = pendulum.parse(startDate,tz=None)
    if not endDate:
        endDate = pendulum.date(2100,1,1)
    else:
        endDate = pendulum.parse(endDate,tz=None)

    logger.info(f'loading valid trading days for period {startDate.to_date_string()} - {endDate.to_date_string()}')
    ecal = nyseCal(startDate, endDate)

    serTradeDates=pd.Series(ecal.sessions.to_pydatetime().tolist(),name='sessions')

    logger.info(f'{len(serTradeDates.to_list())} trading days have been loaded.')

    return serTradeDates.to_list()

import pandas as pd
import matplotlib.pyplot as plt


# --------------------------------------------------------------------------------------------------
# plot histogram of errPnl

def fnPlotHist(df, colToPlot = 'errPnl', bins = 12):
    # put into 12 evenly spaced buckets
    df['bucket'] = pd.cut(df[colToPlot], bins)

    # add up using groupby
    newdf = df[['bucket', colToPlot]].groupby('bucket').sum()
    newdf.plot(kind = 'bar', edgecolor = 'black')
    return plt.show()


# --------------------------------------------------------------------------------------------------
# errPnl descriptive stats by accnt

def errPnlAccntStatistics(df, bins = 12, minRange = -100, maxRange = 100):
    dfA = df.groupby('accnt')['errPnl'].sum()

    print('\nError PnL grouped by accnt for April 1st - 30th: ')
    print('mean: %s' % dfA.mean().round(2))
    print('std: %s' % dfA.std().round(2))
    print('skew: %s' % dfA.skew().round(2))
    print('kurtosis: %s' % dfA.kurtosis().round(2))

    plt.hist(dfA, bins = bins, range = (minRange, maxRange), edgecolor = 'black')
    return plt.show()

# stkUtils.py functions
#
# --------------------------------------------------------------------------------------------------
# created by joe.loss

# --------------------------------------------------------------------------------------------------
# total return

def totalReturn(prices):
    """Returns the return between the first and last value of the dataframe"""
    return prices.iloc[-1] / prices.iloc[0] - 1

def totalReturnFromReturns(returns):
    """Returns the return from returns"""
    return (returns + 1).prod() - 1

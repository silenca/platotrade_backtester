import datetime

from numba import jit
from dateutil.tz import tzlocal

def handling_coefficient(data, macd):
    data = data[macd.time_period]
    stock = macd.calculate_coefficient(data)
    signal = stock['macds']  # Your signal line
    macd = stock['macd']  # The MACD that need to cross the signal line
    #                                              to give you a Buy/Sell signal
    listLongShort = ["No data"]  # Since you need at least two days in the for loop
    for i in range(1, len(signal)):
        # If the MACD crosses the signal line upward
        if macd.iloc[i] > signal.iloc[i] and macd.iloc[i - 1] <= signal.iloc[i - 1]:
            listLongShort.append("BUY")
        # The other way around
        elif macd.iloc[i] < signal.iloc[i] and macd.iloc[i - 1] >= signal.iloc[i - 1]:
            listLongShort.append("SELL")
        # Do nothing if not crossed
        else:
            listLongShort.append("HOLD")

    stock['advice'] = listLongShort
    # The advice column means "Buy/Sell/Hold" at the end of this day or
    #  at the beginning of the next day, since the market will be closed

    # print(stock.head('Advise')=listLo)
    last_calculation = stock.tail(1)
    last_calculation.index.name = 'date'
    last_calculation = last_calculation.reset_index()
    now = datetime.datetime.now(tzlocal())
    last_calculation.index = [now.strftime('%Y-%m-%d %H:%M:%S')]
    return last_calculation


@jit
def calc_advise(stock):
    signal = stock['macds']  # Your signal line
    macd = stock['macd']  # The MACD that need to cross the signal line
    #                                              to give you a Buy/Sell signal
    listLongShort = ["No data"]  # Since you need at least two days in the for loop

    for i in range(1, len(signal)):
        #                          # If the MACD crosses the signal line upward
        if macd.values[i] > signal.values[i] and macd.values[i - 1] <= signal.values[i - 1]:
            listLongShort.append("BUY")
        # # The other way around
        elif macd.values[i] < signal.values[i] and macd.values[i - 1] >= signal.values[i - 1]:
            listLongShort.append("SELL")
        # # Do nothing if not crossed
        else:
            listLongShort.append("HOLD")

    # stock['Advice'] = listLongShort
    # The advice column means "Buy/Sell/Hold" at the end of this day or
    #  at the beginning of the next day, since the market will be closed

    # print(stock.head('Advise')=listLo)
    return listLongShort


if __name__ == '__main__':
    from app.services.macd import MACD

    macd = MACD('btc_usd', 12, 26, 9, 15, 1)
    stock = macd.get_data(1524873600, 15254784000)
    stock = macd.calculate_coefficient(stock)
    print(calc_advise(stock))

import requests
import pandas as pd

from stockstats import StockDataFrame as Sdf

import datetime


def trading_view_data():
    params = {'symbol': 49798,
              'resolution': 5,
              'from': 1517739932,
              'to': 1519035992}
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36'}
    r = requests.get(
        'https://tvc4.forexpros.com/1e8507a55858eeadbfe47be9c57387b3/1518710727/1/1/8/history', params=params,
        headers=headers)
    tr_data = r.json()
    data = pd.DataFrame()
    data['date'] = list(
        map(lambda x: datetime.datetime.fromtimestamp(int(x)).strftime('%Y-%m-%d %H:%M:%S'), tr_data['t']))
    data['volume'] = tr_data['vo']
    data['high'] = tr_data['h']
    data['close'] = tr_data['c']
    data['open'] = tr_data['o']
    data['low'] = tr_data['l']
    # data['amount'] = tr_data['v']
    return data



df = pd.read_csv('main_ratefifteen.csv')

data = df.rename(columns={"vo": 'volume', "h": "high", 'c': 'close', 'o': 'open', 'l': 'low'})
data = data.sort_values('create')

def calculate_macd_param(set_macd=[12,26,9]):
    sdf = Sdf
    sdf.MACD = set_macd
    stock = sdf.retype(data)
    return stock

stock = calculate_macd_param()
signal = stock['macds']  # Your signal line
macd = stock['macd']  # The MACD that need to cross the signal line
#                                              to give you a Buy/Sell signal
listLongShort = ["No data"]  # Since you need at least two days in the for loop

for i in range(1, len(signal)):
    #                          # If the MACD crosses the signal line upward
    if macd.iloc[i] > signal.iloc[i] and macd.iloc[i - 1] <= signal.iloc[i - 1]:
        listLongShort.append("BUY")
    # # The other way around
    elif macd.iloc[i] < signal.iloc[i] and macd.iloc[i - 1] >= signal.iloc[i - 1]:
        listLongShort.append("SELL")
    # # Do nothing if not crossed
    else:
        listLongShort.append("HOLD")

# stock['Advice'] = listLongShort
# The advice column means "Buy/Sell/Hold" at the end of this day or
#  at the beginning of the next day, since the market will be closed

# print(stock.head('Advise')=listLo)
data['Advisor'] = listLongShort


data.to_csv('result.csv')

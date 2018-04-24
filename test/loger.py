import os
import time

from macd import MACD
from test.adviser import handling_coefficient

macd_objects = []
server = 'http://localhost:5000'


def get_data():
    from utils import fetch, parse_data
    data = fetch('btc_usd')
    d = parse_data(data)
    return d


for i in [1, 15, 30, 60, 1440]:
    # plato = {'pair': 'BTC_USD',
    #          'fast_period': 12,
    #          'slow_period': 26,
    #          'signal_period': 9,
    #          'time_period': i,
    #          'plato_ids': i}
    #
    # requests.put(f'{server}/addplato', params=plato)

    macd_objects.append(MACD('btc_usd', 12, 26, 9, str(i), i))


def logger(df, period):
    file_name = f'advise_{period}.csv'
    if not os.path.isfile(file_name):
        df.to_csv(file_name, header='column_names')
    else:  # else it exists so append without writing the header
        df.to_csv(file_name, mode='a', header=False)


def call_all():
    data = get_data()
    signals = []
    for i in macd_objects:
        last_row = handling_coefficient(data, i)
        if last_row['advise'] != 'HOLD':
            logger(last_row, i.time_period)
            signals.append(last_row)
    return signals

if __name__ == '__main__':
    while True:
        call_all()
        time.sleep(10)

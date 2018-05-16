import time
import csv
import collections
import pandas as pd
from itertools import product

from app.macd import MACD
from app.utils import fetch, parse_date_period
from test.adviser import calc_advise


FAST_PERIOD = range(2, 30)
SLOW_PERIOD = range(10, 40)
SIGNAL_PERIOD = range(2, 20)
INTERVALS = [15, 30, 60, 120, 240, 1440]


def backtest_all(from_, to_, pair):
    cache_data_by_period = {}
    filename = f'backtest-{time.strftime("%Y-%m-%d %H_%M_%S")}.csv'

    for period in INTERVALS:
        data = fetch(pair, time_period={'from': from_, 'to': to_}, interval=period)
        cache_data_by_period[period] = parse_date_period(data)

    items = product(FAST_PERIOD, SLOW_PERIOD, SIGNAL_PERIOD, INTERVALS)

    statistics_fields = ['time', 'wins', 'losses', 'total',
                         'total_wins', 'total_losses',
                         'fees', 'macd_parameters']

    with open(filename, 'a') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=statistics_fields)
        writer.writeheader()

    def run(item):
        macd = MACD(pair, *item, plato_ids=None)
        data = cache_data_by_period[item[3]]
        stock = macd.calculate_coefficient(data)
        stock['advise'] = calc_advise(stock)
        backtest = Backtest(stock, stock)
        trades = backtest.calc_trades()
        statistics = backtest.get_statistics(trades)
        statistics['macd_parameters'] = item
        statistics['time'] = time.strftime('%H:%M:%S')
        with open(filename, 'a') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=statistics_fields)
            writer.writerow(statistics)

    collections.deque(map(run, items))


class Backtest:
    COMMISSION = 0.002

    def __init__(self, buy_data, sell_data):
        self.buy = buy_data
        self.sell = sell_data
        self.capital = None
        self.lot_size = 1

    def calc_trades(self):
        trades = []
        buy = self.buy.loc[self.buy['advise'].isin(['BUY', "SELL"])]
        sell = self.sell.loc[self.sell['advise'].isin(["SELL"])]
        signals = pd.concat([buy, sell]).sort_values(['minute_ts'])
        wait_signal = 'BUY'

        for index, row in signals.iterrows():
            if 'BUY' == wait_signal and row['advise'] == 'BUY':
                if self.capital is None:
                    self.capital = float(row.get('close'))
                self.lot_size = (1 - self.COMMISSION) * (self.capital / float(row.get('close')))
                trades.append({'price_enter': row.get('close'),
                               'ts_enter': row.get('minute_ts'),
                               'capital': self.capital,
                               'amount': self.lot_size,
                               'fee_enter': self.capital * self.COMMISSION})
                wait_signal = 'SELL'

            elif 'SELL' == wait_signal and row['advise'] == wait_signal:
                income = self.lot_size * float(row.get('close'))
                fee_exit = income * self.COMMISSION
                income -= fee_exit

                trades[-1].update({'price_exit': float(row.get('close')),
                                   'delta': income - trades[-1]['capital'],
                                   'ts_exit': row.get('minute_ts'),
                                   'fee_exit': fee_exit,
                                   })
                wait_signal = 'BUY'
                self.capital = income
            else:
                pass
        if wait_signal == 'SELL':
            trades.pop(-1)
        return trades

    def get_statistics(self, trades):
        wins = 0
        losses = 0
        total = 0
        total_wins = 0
        total_losses = 0
        fee = 0
        for trade in trades:
            total += trade['delta']
            fee += (trade['fee_enter'] + trade['fee_exit'])
            if trade['delta'] > 0:
                wins += 1
                total_wins += trade['delta']
            else:
                losses += 1
                total_losses += trade['delta']
        return {'wins': wins,
                'losses': losses,
                'total': total,
                'total_wins': total_wins,
                'total_losses': total_losses,
                'fees': fee}


if __name__ == '__main__':
    backtest_all(1525132800, 1526299200, 'BTC_USD')

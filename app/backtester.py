import datetime
import collections
import pandas as pd
import copy
from itertools import product

from app.macd import MACD
from app.utils import fetch, parse_date_period
from test.adviser import calc_advise
from app.models.backtest import Backtest

FAST_PERIOD = range(2, 30)
SLOW_PERIOD = range(10, 40)
SIGNAL_PERIOD = range(2, 20)
# INTERVALS = [15, 30, 60, 120, 240, 1440]
INTERVALS = [1440]


def backtest_all(from_, to_, pair):
    cache_data_by_period = {}

    for period in INTERVALS:
        data = fetch(pair, time_period={'from': from_, 'to': to_}, interval=period)
        cache_data_by_period[period] = parse_date_period(data)

    items = product(FAST_PERIOD, SLOW_PERIOD, SIGNAL_PERIOD, INTERVALS)

    def run(item):
        macd = MACD(pair, *item, plato_ids=None)
        data = cache_data_by_period[item[3]]
        stock = macd.calculate_coefficient(data)
        stock['advise'] = calc_advise(stock)
        backtest = Backtester(stock, stock)
        trades = backtest.calc_trades()
        statistics = backtest.get_statistics(trades, to_)

        Backtest.new_backtest(item, item, statistics, from_, to_)

    collections.deque(map(run, items))


class Backtester:
    COMMISSION = 0.002
    WEEK = 14
    MONTH1 = 30
    MONTH3 = MONTH1 * 3
    MONTH6 = MONTH1 *6

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
                # if self.capital is None:
                self.capital = float(row.get('close'))
                # self.lot_size = (1 - self.COMMISSION) * (self.capital / float(row.get('close')))
                trades.append({'price_enter': row.get('close'),
                               'ts_enter': row.get('minute_ts'),
                               'capital': self.capital,
                               'amount': self.lot_size,
                               'fee_enter': self.lot_size * self.capital * self.COMMISSION
                               })
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

            else:
                pass
        if wait_signal == 'SELL':
            trades.pop(-1)
        return trades

    def get_statistics(self, trades, start_date):

        start_date = datetime.datetime.fromtimestamp(start_date)
        start_cond = dict(wins=0,
                          losses=0,
                          total=0,
                          total_wins=0,
                          total_losses=0,
                          fee=0)
        statistics = {'week': copy.deepcopy(start_cond),
                      'month1': copy.deepcopy(start_cond),
                      'month3': copy.deepcopy(start_cond),
                      'month6': copy.deepcopy(start_cond)}

        def calc_trade(statistics_by_period, trade):
            statistics_by_period['total'] += trade['delta']
            statistics_by_period['fee'] += (trade['fee_enter'] + trade['fee_exit'])
            if trade['delta'] > 0:
                statistics_by_period['wins'] += 1
                statistics_by_period['total_wins'] += trade['delta']
            else:
                statistics_by_period['losses'] += 1
                statistics_by_period['total_losses'] += trade['delta']
            return statistics_by_period

        for trade in trades:
            shift_days = (start_date - datetime.datetime.fromtimestamp(int(trade['ts_exit']))).days

            if shift_days <= self.WEEK:
                statistics['week'] = calc_trade(statistics['week'], trade)
            elif shift_days < self.MONTH1:
                statistics['month1'] = calc_trade(statistics['month1'], trade)
            elif shift_days < self.MONTH3:
                statistics['month3'] = calc_trade(statistics['month3'], trade)
            else:
                statistics['month6'] = calc_trade(statistics['month6'], trade)

        # sum periods statistics
        periods = list(statistics.keys())

        for i in range(1, len(periods)):
            for k, v in statistics[periods[i-1]].items():
                 statistics[periods[i]][k] += v

        return statistics


if __name__ == '__main__':
    backtest_all(1514764800, 1526893778, 'BTC_USD')

# RealTime backtest
import argparse
from itertools import product, combinations_with_replacement
from json import dumps
from multiprocessing.pool import Pool
from time import time

import pytz
from pandas import DataFrame
from stockstats import StockDataFrame

from app.models.backtest import Backtest
from app.models.plato import Plato
from app.services.RTBacktest import RTBacktest
from app.services.RateLoader import RateLoader

import datetime

DAY = 24*60*60
INTERVALS = {'day': DAY, 'week': 7*DAY, 'month': 30*DAY, 'month3': 90*DAY, 'month6': 180*DAY}

def tsToTime(ts: int):
    return datetime.datetime.fromtimestamp(ts, tz=pytz.UTC).strftime('%Y-%m-%d %H:%M:%S')

class Gen():

    FAST_PERIOD = [2]#range(2, 30)
    SLOW_PERIOD = [10]#range(10, 40)
    SIGNAL_PERIOD = [2]#range(2, 20)
    INTERVALS = [30, 60]#[30, 60, 120, 240, 1440]

    def __init__(self, pair: str, rates: StockDataFrame, begin: int, end: int):
        self.pair = pair
        self.begin = begin
        self.end = end
        self.rates = rates

    def getItems(self):
        iterators = self.getIterators()

        for f1, s1, si1, f2, s2, si2, (p1, p2) in product(*iterators):
            yield (
                Plato(self.pair, f1, s2, si1, p1),
                Plato(self.pair, f2, s2, si2, p2),
                self.rates,
                self.begin,
                self.end
            )

    def getIterators(self):
        return [
            self.FAST_PERIOD, self.SLOW_PERIOD, self.SIGNAL_PERIOD,
            self.FAST_PERIOD, self.SLOW_PERIOD, self.SIGNAL_PERIOD,
            list(combinations_with_replacement(reversed(self.INTERVALS), 2))[:-1]
        ]

class GenCustom():

    def __init__(self, pair: str, rates: StockDataFrame, begin: int, end: int, params):
        self.pair = pair
        self.begin = begin
        self.end = end
        self.rates = rates
        self.params = params

    def getItems(self):
        for str_item in self.params:
            f1, s1, si1, p1, f2, s2, si2, p2 = list(map(int, str_item.split(':')))
            yield (
                Plato(self.pair, f1, s1, si1, p1),
                Plato(self.pair, f2, s2, si2, p2),
                self.rates,
                self.begin,
                self.end
            )

def calculate(penter: Plato, pexit: Plato, rates: StockDataFrame, begin: int, end: int):
    stats = RTBacktest(penter, pexit, rates, begin, end).run()

    if stats is not None:
        return dict(
            buy_fast=penter.fast,
            buy_slow=penter.slow,
            buy_signal=penter.signal,
            buy_period=penter.period,
            sell_fast=pexit.fast,
            sell_slow=pexit.slow,
            sell_signal=pexit.signal,
            sell_period=pexit.period,
            status='3',
            type='1',
            data=f'{dumps(dict(statistics=stats))}',
            extend='|main.backtest|',
            name=f'[RT] Buy: {penter.key()}, Sell: {pexit.key()}',
            total_month6=float(stats['4']['profit']),
            total_month3=float(stats['3']['profit']),
            total_month1=float(stats['2']['profit']),
            total_week=float(stats['1']['profit']),
            ts_start=begin,
            ts_end=end
        )
    else:
        return None

if __name__ == '__main__':
    tts = time()
    parser = argparse.ArgumentParser(description='RealTime backtest for plato')

    parser.add_argument('--pair', choices=['btc_usd'], help='Pair to test', default='btc_usd')
    parser.add_argument('--goback', choices=INTERVALS.keys(), help='How many days to check', default='month6')
    parser.add_argument('-till', help='Reference timestamp to stop')
    parser.add_argument('--data', help='Combinations f1:s1:si1:p1:f2:s2:si2:p2,...')

    args = parser.parse_args()

    if args.data is None:
        print('No data. Exit')
        exit(0)
    params = args.data.split(',')

    end = int(time())
    if args.till is not None:
        end = int(args.till)

    begin = end - INTERVALS[args.goback]
    begin_with_offset = begin - max(*Gen.INTERVALS)*60*33

    print(f'Start RT backtest')
    print(f'Period: {tsToTime(begin)} -> {tsToTime(end)}')
    ts = time()
    rates = RateLoader().fetchPeriods(args.pair, begin_with_offset, end, [1]).getSdf(args.pair, 1)
    print(f'Rates loaded (~{len(rates)}) in {round(time()-ts, 3)}s')

    generator = GenCustom('btc_usd', rates, begin, end, params);

    backtests = []
    ts = time()
    pool = Pool(processes=4, maxtasksperchild=10)
    backtests = pool.starmap(calculate, generator.getItems())
    pool.close()
    pool.join()

    print(f'Pool calculation takes {round(time()-ts, 3)}s (~{len(list(generator.getItems()))} items)')
    print(f'Saving {len(set(backtests))} backtests')
    ts = time()
    Backtest.saveMany(backtests)
    print(f'Saving takes ~{round(time()-ts, 3)}s')

    total = int(time()-tts)
    total_sec = total%60
    print(f'Total time: {total-total_sec}:{total_sec} minutes')


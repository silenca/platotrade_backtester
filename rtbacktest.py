# RealTime backtest
import argparse
from time import time

import pytz

from app.models.plato import Plato
from app.services.RTBacktest import RTBacktest
from app.services.RateLoader import RateLoader

import datetime

DAY = 24*60*60
INTERVALS = {'day': DAY, 'week': 7*DAY, 'month': 30*DAY, 'month3': 90*DAY, 'month6': 180*DAY}

def tsToTime(ts: int):
    return datetime.datetime.fromtimestamp(ts, tz=pytz.UTC).strftime('%Y-%m-%d %H:%M:%S')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='RealTime backtest for plato')

    parser.add_argument('--pair', choices=['btc_usd'], help='Pair to test', default='btc_usd')
    parser.add_argument('--fast', choices=range(2, 30), type=int, help='Fast MACD param', default=12)
    parser.add_argument('--slow', choices=range(10, 40), type=int, help='Slow MACD param', default=26)
    parser.add_argument('--signal', choices=range(2, 20), type=int, help='Signal MACD param', default=9)
    parser.add_argument('--period', choices=[15, 30, 60, 120, 240, 1440], type=int, help='Time Period to use', default=60)
    parser.add_argument('--goback', choices=INTERVALS.keys(), help='How many days to check', default='month')

    args = parser.parse_args()

    plato = Plato(args.pair, args.fast, args.slow, args.signal, args.period)

    end = int(time())
    begin = end - INTERVALS[args.goback]
    begin_with_offset = begin - plato.period*60*33

    print(f'Start RT backtest for plato #{plato.key()}({plato.pair}) from {tsToTime(begin)} till {tsToTime(end)}')
    ts = time()
    rates = RateLoader().fetchPeriods(plato.pair, begin_with_offset, end, [1]).getSdf(plato.pair, 1)
    print(f'Rates loaded (~{len(rates)}) in {round(time()-ts, 3)}s')

    RTBacktest(plato, rates, begin, end).run()
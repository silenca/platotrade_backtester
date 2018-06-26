import pytz
from functools import reduce
from operator import mul
from app.models.plato import Plato
from pandas import DataFrame, to_datetime
from stockstats import StockDataFrame
from time import time
from config import FileConfig
from dateutil.relativedelta import relativedelta
from datetime import datetime
from itertools import product

class Backtest():
    @staticmethod
    def calculate(penter: Plato, pexit: Plato, denter: StockDataFrame, dexit: StockDataFrame, begin, end, force=None):
        ts = time()
        penter.calculateAll(denter.copy(deep=True))
        pexit.calculateAll(dexit.copy(deep=True))

        statistics = Statistics(begin, end).calculate(penter, pexit);

        del penter.adviseData
        del pexit.adviseData
        del denter
        del dexit

        skip_negative = FileConfig().get('CALC.SKIP_NEGATIVE', True, bool)
        if force == True:
            skip_negative = False

        if skip_negative:
            isPositive = False
            for period in statistics:
                if statistics[period]['total'] > 0:
                    isPositive = True
                    break
        else:
            isPositive = True

        return (penter, pexit, begin, end, statistics if isPositive else None, time() - ts)


class Statistics():

    FEE = 0.002

    STATISTICS_PERIODS = {'1': {'days': -7}, '2': {'months': -1}, '3': {'months': -3}, '4': {'months': -6}}

    def __init__(self, begin: int, till: int, lot_size: float=1.0):
        self.lot_size = lot_size
        self.till = till
        self.begin = begin

    def calculate(self, penter: Plato, pexit: Plato):
        adv_enter = penter.adviseData
        adv_enter = adv_enter[adv_enter.advise == Plato.ADVISE_BUY][['close', 'minute_ts', 'advise']]

        adv_exit = pexit.adviseData
        last_price = (adv_exit.minute_ts.values[-1], adv_exit.close.values[-1])
        adv_exit = adv_exit[adv_exit.advise == Plato.ADVISE_SELL][['close', 'minute_ts', 'advise']]

        adv_comb = adv_enter.combine_first(adv_exit)
        adv_comb.minute_ts = adv_comb.minute_ts.astype(int)
        advises = adv_comb[adv_comb.minute_ts >= self.begin].copy()

        del adv_enter
        del adv_exit
        del adv_comb

        deals = DataFrame(columns=['price_enter', 'ts_enter', 'price_exit', 'ts_exit'])
        deal = None

        for adv in advises.itertuples():
            if deal is None:
                if adv.advise == Plato.ADVISE_BUY:
                    deal = dict(
                        ts_enter=adv.minute_ts,
                        price_enter=adv.close
                    )
            else:
                if adv.advise == Plato.ADVISE_SELL:
                    deal['price_exit'] = adv.close
                    deal['ts_exit'] = adv.minute_ts

                    deals = deals.append(DataFrame(deal, index=[adv.minute_ts]), ignore_index=True)
                    deal = None

        if deal is not None and last_price is not None:
            ts, price = last_price
            if deal['ts_enter'] != ts:
                deal['price_exit'] = price
                deal['ts_exit'] = ts

                deals = deals.append(DataFrame(deal, index=[deal['ts_exit']]), ignore_index=True)

        del advises

        deals.price_enter = deals.price_enter.astype(float)
        deals.price_exit = deals.price_exit.astype(float)
        deals.ts_enter = deals.ts_enter.astype(int)
        deals.ts_exit = deals.ts_exit.astype(int)
        deals['dealTs'] = deals.ts_exit

        deals['fees'] = (deals['price_enter'] + deals['price_exit']) * self.FEE
        deals['delta'] = deals['price_exit'].values - deals['price_enter'].values
        deals['ts_enter'] = to_datetime(deals['ts_enter'], unit='s', utc=True).dt.strftime('%Y-%m-%d %H:%M:%S')
        deals['ts_exit'] = to_datetime(deals['ts_exit'], unit='s', utc=True).dt.strftime('%Y-%m-%d %H:%M:%S')

        dateTo = datetime.fromtimestamp(self.till, tz=pytz.UTC)

        statistics = {}

        for idx, period in self.STATISTICS_PERIODS.items():
            dateLimit = (dateTo + relativedelta(**period)).timestamp()
            stGroup = deals[deals.dealTs >= dateLimit]
            if stGroup.empty:
                statistics[idx] = dict(
                    fees=0, profit=0, total=0, totalWins=0,
                    totalLosses=0, wins='0', losses='0', trades='0', calcs=[]
                )
                continue

            stGroup.insert(0, 'interval', str(idx))
            stGroup.insert(0, 'amount', self.lot_size)

            winsGroup = stGroup[stGroup.delta > 0]
            lossesGroup = stGroup[stGroup.delta <= 0]

            fees = round(stGroup['fees'].sum(), 2)
            total = round(stGroup['delta'].sum(), 2)
            wins = round(winsGroup['delta'].sum(), 2)

            statistics[idx] = dict(
                fees=fees,
                profit=total - fees,
                total=total,
                totalWins=wins,
                totalLosses=total - wins,
                wins='%d' % winsGroup['delta'].count(),
                losses='%d' % lossesGroup['delta'].count(),
                trades='%d' % stGroup['delta'].count(),
                calcs=stGroup.to_dict('index')
            )

        del deals

        return statistics

class Generator():

    FAST_PERIOD = range(2, 30)
    SLOW_PERIOD = range(10, 40)
    SIGNAL_PERIOD = range(2, 20)
    INTERVALS = [30, 60, 120, 240, 1440]

    def __init__(self, pair: str, rates: StockDataFrame, begin: int, end: int):
        self.pair = pair
        self.begin = begin
        self.end = end
        self.rates = rates

    def getItems(self):
        iterators = self.getIterators()

        rates = self.rates

        def gen(data):
            fast, slow, signal, (bperiod, speriod) = data
            return (
                Plato(self.pair, fast, slow, signal, bperiod),
                Plato(self.pair, fast, slow, signal, speriod),
                rates[bperiod],
                rates[speriod],
                self.begin,
                self.end
            )

        for item in product(*iterators):
            fast, slow, _, (bperiod, speriod) = item
            if fast >= slow:
                continue
            if bperiod < 60 or bperiod < speriod:
                continue
            yield gen(item)

    def countItems(self):
        return reduce(mul, map(lambda x:len(list(x)), self.getIterators()), 1)

    def getIterators(self):
        return [self.FAST_PERIOD, self.SLOW_PERIOD, self.SIGNAL_PERIOD, product(self.INTERVALS, self.INTERVALS)]

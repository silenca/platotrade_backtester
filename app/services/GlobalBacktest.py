import pytz
from pandas import DataFrame, Series, to_datetime
from json import dumps

from stockstats import StockDataFrame

from app.services.RateLoader import RateLoader
from app.models.plato import Plato
from itertools import product, combinations_with_replacement
from time import time
from datetime import datetime
from multiprocessing import Pool
from functools import reduce
from operator import mul
from dateutil.relativedelta import relativedelta
from app.models.backtest import Backtest

import logging

class GlobalBacktest:
    STATISTICS_PERIODS = {'1': {'days': -7}, '2': {'months': -1}, '3': {'months': -3}, '4': {'months': -6}}

    FEE = 0.002

    def __init__(self, pair, tsFrom, tsTo, lot_size:int = 1):
        ts = time()
        self.tsFrom = tsFrom
        self.tsTo = tsTo
        self.pair = pair
        self.lot_size = lot_size
        self.logger = logging.getLogger('bt')

        rateData = RateLoader().fetchPeriods(self.pair, self.tsFrom, self.tsTo, Generator.INTERVALS)
        self.rateData = rateData.getPairSdf(self.pair)
        self.log(f'All data loaded in {time() - ts}s')

    def log(self, msg):
        self.logger.info(msg)

    def run(self):
        ts = time()

        generator = Generator(self.pair, self.rateData, self.tsFrom, self.tsTo)

        pool = Pool(processes=4, maxtasksperchild=10)
        positiveCalculations = pool.starmap(Tester.calculate, generator.getItems())
        #positiveCalculations = deque(self.smap(Tester.calculate, generator.getItems()))
        pool.close()
        pool.join()
        self.log('Pool calculation is done in '+'%.3fs'%(time()-ts))

        backtests = []
        self.log(f'There was {len(positiveCalculations)} items')
        for data in positiveCalculations:
            penter, pexit, tsFrom, tsTo, statistics, elapsed = data
            if statistics is not None:
                backtests.append(dict(
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
                    data=f'{dumps(dict(statistics=statistics))}',
                    extend='|main.backtest|',
                    name=f'Buy: {penter.key(":")}, Sell: {pexit.key(":")}',
                    total_month6=round(float(statistics['4']['profit']), 2),
                    total_month3=round(float(statistics['3']['profit']), 2),
                    total_month1=round(float(statistics['2']['profit']), 2),
                    total_week=round(float(statistics['1']['profit']), 2),
                    ts_start=self.tsFrom,
                    ts_end=self.tsTo
                ))
        self.log(f'Saving {len(backtests)} backtests');
        ts = time()
        Backtest.saveMany(backtests)
        self.log('Saved in %.4fs' % (time() - ts))
        self.log(f'There are {generator.countItems()} items and {(self.tsTo - self.tsFrom)//(24*60*60)} days')
        return generator.countItems();

    @staticmethod
    def calculate(plato, stockData: StockDataFrame, begin, end):
        ts = time()
        plato.calculateAll(stockData)

        statistics = StatisticsCalc(end).calculate(plato);

        isPositive = False
        for period in statistics:
            if statistics[period]['totalAmount'] > 0:
                isPositive = True
                break

        return (plato, begin, end, statistics if isPositive else None, time()-ts)

    def calculateDealStatistics(self, plato:Plato):
        dfDeals = DataFrame(columns=['price_enter', 'price_exit', 'ts_enter', 'ts_exit', 'dealTs'])

        buy = None
        for date, advise in plato.adviseData.iterrows():
            if buy is None:
                if advise['advise'] == Plato.ADVISE_BUY:
                    buy = advise
            elif advise['advise'] == Plato.ADVISE_SELL:
                dfDeals.loc[date] = Series({
                    'price_enter': float(buy['close']),
                    'price_exit': float(advise['close']),
                    'ts_enter': int(buy['minute_ts']),
                    'ts_exit': int(advise['minute_ts']),
                    'dealTs': int(advise['minute_ts'])
                })
                buy = None

        dfDeals['fees'] = (dfDeals['price_enter'] + dfDeals['price_exit'])*self.FEE
        dfDeals['delta'] = dfDeals['price_exit'] - dfDeals['price_enter']

        dfDeals['ts_enter'] = to_datetime(dfDeals['ts_enter'], unit='s', utc=True).dt.strftime('%Y-%m-%d %H:%M:%S')
        dfDeals['ts_exit'] = to_datetime(dfDeals['ts_exit'], unit='s', utc=True).dt.strftime('%Y-%m-%d %H:%M:%S')

        dateTo = self.tsTo
        #dateTo = datetime.today() # Temporary. To fit curent implementation

        statistics = {}

        for idx, period in self.STATISTICS_PERIODS.items():
            dateLimit = (dateTo + relativedelta(**period)).timestamp()
            stGroup = dfDeals[dfDeals.dealTs >= dateLimit]
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

        return statistics

    def getItems(self, pair):
        begin = self.tsFrom
        end = self.tsTo
        iterators = self.getIterators(pair)
        items = product(*iterators)

        def gen(data):
            pair, fast, slow, signal, (period, rateData) = data
            return (Plato(pair, fast, slow, signal, period), rateData, begin, end)

        for item in items:
            yield gen(item)

    def countItems(self, pair):
        return reduce(mul, map(len, self.getIterators(pair)), 1);

    def getIterators(self, pair):
        sdfs = self.rateData.getPairSdf(pair)
        return [[pair], self.FAST_PERIOD, self.SLOW_PERIOD, self.SIGNAL_PERIOD, sdfs.items()]

    def smap(self, func, items):
        for item in items:
            return func(*item)

class Tester():
    @staticmethod
    def calculate(penter: Plato, pexit: Plato, denter: StockDataFrame, dexit: StockDataFrame, begin, end):
        ts = time()
        penter.calculateAll(denter)
        pexit.calculateAll(dexit)

        statistics = StatisticsCalc(end).calculate(penter, pexit);

        del penter.adviseData
        del pexit.adviseData
        del denter
        del dexit

        isPositive = False
        for period in statistics:
            if statistics[period]['total'] > 0:
                isPositive = True
                break

        return (penter, pexit, begin, end, statistics if isPositive else None, time() - ts)

class StatisticsCalc():
    FEE = 0.002

    STATISTICS_PERIODS = {'1': {'days': -7}, '2': {'months': -1}, '3': {'months': -3}, '4': {'months': -6}}

    def __init__(self, till: int, lot_size: float=1.0):
        self.lot_size = lot_size
        self.till = till

    def calculate(self, penter: Plato, pexit: Plato):
        adv_enter = penter.adviseData
        adv_enter = adv_enter[adv_enter.advise == Plato.ADVISE_BUY][['close', 'minute_ts', 'advise']]

        adv_exit = pexit.adviseData
        adv_exit = adv_exit[adv_exit.advise == Plato.ADVISE_SELL][['close', 'minute_ts', 'advise']]

        advises = adv_enter.combine_first(adv_exit)
        #advises = advises[:10]# DEBUG

        advises[['price_enter', 'ts_enter']] = advises[advises.advise == Plato.ADVISE_BUY][['close', 'minute_ts']]
        advises[['price_exit', 'ts_exit']] = advises[advises.advise == Plato.ADVISE_SELL][['close', 'minute_ts']]
        advises['ts_exit'] = advises['ts_exit'].shift(-1)
        advises['price_exit'] = advises['price_exit'].shift(-1)

        if not advises.empty:
            if advises.iat[0, 2] == Plato.ADVISE_SELL:
                deals = DataFrame(advises[1:].dropna())
            else:
                deals = DataFrame(advises.dropna())
        else:
            deals = DataFrame(advises.dropna())

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

        def gen(data):
            pair, fast, slow, signal, (bperiod, speriod), ratesData = data
            return (
                Plato(pair, fast, slow, signal, bperiod),
                Plato(pair, fast, slow, signal, speriod),
                ratesData[bperiod],
                ratesData[speriod],
                self.begin,
                self.end
            )

        for item in product(*iterators):
            yield gen(item)

    def countItems(self):
        return reduce(mul, map(len, self.getIterators()), 1)

    def getIterators(self):
        periods = list(combinations_with_replacement(reversed(self.INTERVALS), 2))[:-1]
        return [[self.pair], self.FAST_PERIOD, self.SLOW_PERIOD, self.SIGNAL_PERIOD, periods, [self.rates]]

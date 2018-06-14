from collections import deque
from copy import deepcopy

from pandas import DataFrame, Series, to_datetime
from json import dump

from stockstats import StockDataFrame

from app.models.RateData import RateData
from app.services.RateLoader import RateLoader
from app.models.plato import Plato
from itertools import product
from time import time
from datetime import datetime
from multiprocessing import Pool
from functools import reduce
from operator import mul
from dateutil.relativedelta import relativedelta
from app.models.backtest import Backtest

class GlobalBacktest:
    STATISTICS_PERIODS = {'1': {'days': -7}, '2': {'months': -1}, '3': {'months': -3}, '4': {'months': -6}}

    FEE = 0.002

    def __init__(self, pair, tsFrom, tsTo, lot_size:int = 1):
        ts = time()
        self.tsFrom = tsFrom
        self.tsTo = tsTo
        self.pair = pair
        self.lot_size = lot_size

        rateData = RateLoader().fetchPeriods(self.pair, self.tsFrom, self.tsTo, Generator.INTERVALS)
        self.rateData = rateData.getPairSdf(self.pair)
        print(f'All data loaded in {time() - ts}s')

    def run(self):
        ts = time()

        generator = Generator(self.pair, self.rateData, self.tsFrom, self.tsTo)

        pool = Pool(processes=8, maxtasksperchild=5)
        positiveCalculations = pool.starmap(Tester.calculate, generator.getItems())
        #positiveCalculations = deque(self.smap(Tester.calculate, generator.getItems()))
        pool.close()
        pool.join()
        print('Pool calculation is done in '+'%.3fs'%(time()-ts))

        backtests = []
        print(f'There was {len(positiveCalculations)} items')
        for data in positiveCalculations:
            plato, tsFrom, tsTo, statistics, elapsed = data
            if statistics is not None:
                backtests.append({
                    'buy_fast': plato.fast,
                    'buy_slow': plato.slow,
                    'buy_signal': plato.signal,
                    'buy_period': plato.period,
                    'sell_fast': plato.fast,
                    'sell_slow': plato.slow,
                    'sell_signal': plato.signal,
                    'sell_period': plato.period,
                    'status': '3',
                    'type': '1',
                    'data': f'{statistics}',
                    'extend': 'main.backtest',
                    'name': f'Buy: {plato.key()}, Sell: {plato.key()}',
                    'total_month6': float(statistics['4']['totalAmount']),
                    'total_month3': float(statistics['3']['totalAmount']),
                    'total_month1': float(statistics['2']['totalAmount']),
                    'total_week': float(statistics['1']['totalAmount']),
                    'ts_start': tsFrom,
                    'ts_end': tsTo
                })
        print(f'Saving {len(backtests)} backtests');

        ts = time()
        #Backtest.saveMany(backtests)
        print('Saved in %.4fs' % (time() - ts))
        print(f'There are {generator.countItems()} items and {(self.tsTo - self.tsFrom)//(24*60*60)} days')
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
        dateTo = datetime.today() # Temporary. To fit curent implementation

        statistics = {}

        for idx, period in self.STATISTICS_PERIODS.items():
            dateLimit = (dateTo + relativedelta(**period)).timestamp()
            stGroup = dfDeals[dfDeals.dealTs >= dateLimit]
            if stGroup.empty:
                statistics[idx] = dict(
                    feesAmount=0, totalAmount=0, winsAmount=0, lossesAmount=0,
                    wins='0', losses='0', total='0', calcs=[]
                )
                continue

            stGroup.insert(0, 'interval', str(idx))
            stGroup.insert(0, 'amount', self.lot_size)

            winsGroup = stGroup[stGroup.delta > 0]
            lossesGroup = stGroup[stGroup.delta <= 0]

            statistics[idx] = {
                'feesAmount': round(stGroup['fees'].sum(), 2),
                'totalAmount': round(stGroup['delta'].sum(), 2),
                'winsAmount': round(winsGroup['delta'].sum(), 2),
                'lossesAmount': round(abs(lossesGroup['delta'].sum()), 2),
                'wins': '%d' % winsGroup['delta'].count(),
                'losses': '%d' % lossesGroup['delta'].count(),
                'total': '%d' % stGroup['delta'].count(),
                'calcs': []#stGroup.to_dict('index')
            }

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
    def calculate(plato, stockData: StockDataFrame, begin, end):
        ts = time()
        plato.calculateAll(stockData)

        statistics = StatisticsCalc(end).calculate(plato);

        isPositive = False
        for period in statistics:
            if statistics[period]['totalAmount'] > 0:
                isPositive = True
                break

        return (plato, begin, end, statistics if isPositive else None, time() - ts)


class StatisticsCalc():
    FEE = 0.002

    STATISTICS_PERIODS = {'1': {'days': -7}, '2': {'months': -1}, '3': {'months': -3}, '4': {'months': -6}}

    def __init__(self, till: int, lot_size: float=1.0):
        self.lot_size = lot_size
        self.till = till

    def calculate(self, plato: Plato):
        advises = plato.adviseData
        advises = advises[advises.advise != Plato.ADVISE_NONE]
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

        #dateTo = self.tsTo
        dateTo = datetime.today()  # Temporary. To fit curent implementation

        statistics = {}

        for idx, period in self.STATISTICS_PERIODS.items():
            dateLimit = (dateTo + relativedelta(**period)).timestamp()
            stGroup = deals[deals.dealTs >= dateLimit]
            if stGroup.empty:
                statistics[idx] = dict(
                    feesAmount=0, totalAmount=0, winsAmount=0, lossesAmount=0,
                    wins='0', losses='0', total='0', calcs=[]
                )
                continue

            stGroup.insert(0, 'interval', str(idx))
            stGroup.insert(0, 'amount', self.lot_size)

            winsGroup = stGroup[stGroup.delta > 0]
            lossesGroup = stGroup[stGroup.delta <= 0]

            statistics[idx] = {
                'feesAmount': round(stGroup['fees'].sum(), 2),
                'totalAmount': round(stGroup['delta'].sum(), 2),
                'winsAmount': round(winsGroup['delta'].sum(), 2),
                'lossesAmount': round(abs(lossesGroup['delta'].sum()), 2),
                'wins': '%d' % winsGroup['delta'].count(),
                'losses': '%d' % lossesGroup['delta'].count(),
                'total': '%d' % stGroup['delta'].count(),
                'calcs': []  # stGroup.to_dict('index')
            }

        return statistics

class Generator():

    FAST_PERIOD = range(2, 30)
    SLOW_PERIOD = range(10, 40)
    SIGNAL_PERIOD = range(2, 20)
    INTERVALS = [15, 30, 60, 120, 240, 1440]

    def __init__(self, pair: str, rates: StockDataFrame, begin: int, end: int):
        self.pair = pair
        self.begin = begin
        self.end = end
        self.rates = rates

    def getItems(self):
        iterators = self.getIterators()

        def gen(data):
            pair, fast, slow, signal, (period, rateData) = data
            return (Plato(pair, fast, slow, signal, period), rateData, self.begin, self.end)

        for item in product(*iterators):
            yield gen(item)

    def countItems(self):
        return reduce(mul, map(len, self.getIterators()), 1)

    def getIterators(self):
        return [[self.pair], self.FAST_PERIOD, self.SLOW_PERIOD, self.SIGNAL_PERIOD, self.rates.items()]

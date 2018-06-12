from pandas import DataFrame, Series, to_datetime
from json import dump
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

    FAST_PERIOD = range(2, 30)
    SLOW_PERIOD = range(10, 40)
    SIGNAL_PERIOD = range(2, 20)
    INTERVALS = [15, 30, 60, 120, 240, 1440]

    STATISTICS_PERIODS = {'1': {'days': -7}, '2': {'months': -1}, '3': {'months': -3}, '4': {'months': -6}}

    FEE = 0.002

    def __init__(self, pair, tsFrom, tsTo, lot_size:int = 1):
        ts = time()
        self.tsFrom = tsFrom
        self.tsTo = tsTo
        self.pair = pair
        self.lot_size = lot_size

        self.rateData = RateLoader().fetchPeriods(self.pair, self.tsFrom, self.tsTo, self.INTERVALS)
        print(f'All data loaded in {time() - ts}s')

    def run(self):
        iterators = [self.FAST_PERIOD, self.SLOW_PERIOD, self.SIGNAL_PERIOD, self.INTERVALS]
        items = product(*iterators)

        pool = Pool(processes=8, maxtasksperchild=5)
        positiveCalculations = pool.starmap(self.calculate, self.getItems())
        pool.close()
        pool.join()

        backtests = []
        for data in positiveCalculations:
            plato, tsFrom, tsTo, statistics = data
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
        Backtest.saveMany(backtests)
        print('Saved in %.4fs' % (time() - ts))
        return reduce(mul, map(len, iterators), 1);

    def calculate(self, plato, stockData):
        ts = time()
        plato.calculateAll(stockData)

        statistics = self.calculateDealStatistics(plato);

        isPositive = False
        for period in statistics:
            if statistics[period]['totalAmount'] > 0:
                isPositive = True
                break

        return (plato, self.tsFrom, self.tsTo, statistics if isPositive else None)

    def calculateDealStatistics(self, plato):
        dfDeals = DataFrame(columns=['price_enter', 'price_exit', 'ts_enter', 'ts_exit', 'dealTs'])

        buy = None
        buyTs = None
        for date, advise in plato.advises.items():
            if buy is None:
                if advise['advise'] == Plato.ADVISE_BUY:
                    buy = advise
                    buyTs = date
            elif advise['advise'] == Plato.ADVISE_SELL:
                dfDeals.loc[date] = Series({
                    'price_enter': float(buy['close']),
                    'price_exit': float(advise['close']),
                    'ts_enter': int(buyTs),
                    'ts_exit': int(date),
                    'dealTs': int(date)
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
            stGroup['interval'] = str(idx)
            stGroup['amount'] = self.lot_size

            winsGroup = stGroup[stGroup.delta > 0]
            lossesGroup = stGroup[stGroup.delta <= 0]

            try:
                statistics[idx] = {
                    'feesAmount': round(stGroup['fees'].sum(), 2),
                    'totalAmount': round(stGroup['delta'].sum(), 2),
                    'winsAmount': round(winsGroup['delta'].sum(), 2),
                    'lossesAmount': round(abs(lossesGroup['delta'].sum()), 2),
                    'wins': '%d' % winsGroup['delta'].count(),
                    'losses': '%d' % lossesGroup['delta'].count(),
                    'total': '%d' % stGroup['delta'].count(),
                    'calcs': stGroup.to_dict('index')
                }
            except TypeError:
                print('********** [TYPE ERROR] *************')
                print(f'Plato: {plato.key()}')
                print('Group:', stGroup)


        return statistics

    def getItems(self):
        iterators = [self.FAST_PERIOD, self.SLOW_PERIOD, self.SIGNAL_PERIOD, self.INTERVALS]

        for item in product(*iterators):
            yield (
                Plato(self.pair, item[0], item[1], item[2], item[3]),
                self.rateData.getSdf(self.pair, item[3])
            )
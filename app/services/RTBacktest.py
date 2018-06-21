from datetime import datetime
from time import time

import pytz
from dateutil.relativedelta import relativedelta
from pandas import to_datetime, DataFrame
from stockstats import StockDataFrame

from app.models.plato import Plato

class RTBacktest():

    PERIOD_OFFSET = 33
    FRAME_SIZE = 40

    def __init__(self, plato_enter: Plato, plato_exit: Plato, rates: StockDataFrame, begin: int, end: int):
        self.plato_enter = plato_enter
        self.plato_exit = plato_exit

        self.rates = RTBacktest.prepareRates(rates)
        self.begin = begin
        self.end = end

    def run(self):
        deals = DataFrame(columns=['ts_enter', 'ts_exit', 'price_enter', 'price_exit'])
        deal = None
        ts = time()
        i = 0

        deals = RTBacktest.calculateDeals(self.ratesGenerator(), self.plato_enter, self.plato_exit, self.begin)

        stats = StatisticsCalc(self.end).calculate(deals);

        isPositive = True#False
        for period in stats:
            if stats[period]['total'] > 0:
                isPositive = True
                break

        if isPositive:
            return stats
        else:
            return None

    @staticmethod
    def calculateDeals(generator, penter: Plato, pexit: Plato, begin: int):
        deals = DataFrame(columns=['ts_enter', 'ts_exit', 'price_enter', 'price_exit'])
        deal = None
        ts = time()
        i = 0

        for rawFrame in generator:
            cur_ts = rawFrame.index.values[-1]
            if cur_ts < begin:
                del rawFrame
                continue
            i += 1

            if deal is None:
                advises = Calculator.calculateRealtimeAdvise(rawFrame, penter)
                is_buy_advise = advises.advise.values[-1] == Plato.ADVISE_BUY
                if is_buy_advise:
                    deal = dict(
                        ts_enter=cur_ts,
                        price_enter=advises.close.values[-1]
                    )
            else:
                advises = Calculator.calculateRealtimeAdvise(rawFrame, pexit)
                is_sell_advise = advises.advise.values[-1] == Plato.ADVISE_SELL
                if is_sell_advise:
                    deal['ts_exit'] = cur_ts
                    deal['price_exit'] = advises.close.values[-1]

                    deals = deals.append(DataFrame(deal, index=[0]), ignore_index=True)
                    deal = None
        if i == 0:
            i = 1
            print('NO DEALS')
        print(f'Iterations: {i}, Takes: {round(time()-ts, 3)}, Avg: {round((time()-ts)/i, 3)}')
        return deals

    def ratesGenerator(self):
        frameLength = self.FRAME_SIZE*max(self.plato_enter.period, self.plato_enter.period)
        till = len(self.rates) - frameLength
        for begin in range(till):
            end = begin + frameLength
            yield self.rates[begin:end]

    def calculateRealtimeFrame(self, rawFrame: StockDataFrame, plato: Plato):
        return Calculator.calculateRealtimeFrame(rawFrame, plato)

    def calculateRealtimeAdvise(self, rawFrame: StockDataFrame, plato: Plato):
        return Calculator.calculateRealtimeAdvise(rawFrame, plato)

    @staticmethod
    def prepareRates(rates: StockDataFrame):
        for key in rates.columns.values:
            if 'close_' in key:
                del rates[key]

        rates.minute_ts = rates.minute_ts.astype(int)
        rates.close = rates.close.astype(float)
        rates = rates.set_index('minute_ts')

        return rates[['close']]

    def getPeriodSec(self, plato: Plato):
        return Calculator.getPeriodSec(plato)

class Calculator():
    FRAME_SIZE = 40

    c_cache = dict()

    @staticmethod
    def getPeriodSec(plato: Plato):
        return plato.period * 60

    @staticmethod
    def calculateRealtimeFrame(rawFrame: StockDataFrame, plato: Plato):
        lts = rawFrame.index.values[-1]
        key = f'{plato.period}_{lts - lts%Calculator.getPeriodSec(plato)}'

        periodFrame, lastIdx, nextIdx = Calculator.c_cache.get(key, (None, None, None))
        if periodFrame is None:
            periodFrame = rawFrame[rawFrame.index%Calculator.getPeriodSec(plato) == 0][-Calculator.FRAME_SIZE - 1:]
            lastIdx = periodFrame.index.values[-1]
            nextIdx = lastIdx + Calculator.getPeriodSec(plato)
            Calculator.c_cache[key] = (periodFrame, lastIdx, nextIdx)

        if lts > lastIdx and lts < nextIdx:
            frame = rawFrame[-1:]
            frame.index = [nextIdx]
            periodFrame = periodFrame.append(frame, verify_integrity=True)
            del frame

        del rawFrame

        return periodFrame

    @staticmethod
    def calculateRealtimeAdvise(rawFrame: StockDataFrame, plato: Plato):
        return plato.calculateReal(Calculator.calculateRealtimeFrame(rawFrame, plato), 1)

class StatisticsCalc():

    FEE = 0.002

    STATISTICS_PERIODS = {'1': {'days': -7}, '2': {'months': -1}, '3': {'months': -3}, '4': {'months': -6}}

    def __init__(self, till: int, amount: float=1.0):
        self.amount = amount
        self.till = till

    def calculate(self, deals: DataFrame):
        deals['ts'] = deals['ts_exit']

        deals['fees'] = (deals['price_enter'] + deals['price_exit']) * self.FEE
        deals['delta'] = deals['price_exit'].values - deals['price_enter'].values
        deals['ts_enter'] = to_datetime(deals['ts_enter'], unit='s', utc=True).dt.strftime('%Y-%m-%d %H:%M:%S')
        deals['ts_exit'] = to_datetime(deals['ts_exit'], unit='s', utc=True).dt.strftime('%Y-%m-%d %H:%M:%S')

        dateTo = datetime.fromtimestamp(self.till, tz=pytz.UTC)

        statistics = {}

        for idx, period in self.STATISTICS_PERIODS.items():
            dateLimit = (dateTo + relativedelta(**period)).timestamp()
            stGroup = deals[deals.ts >= dateLimit]
            if stGroup.empty:
                statistics[idx] = dict(
                    fees=0, profit=0, total=0, totalWins=0,
                    totalLosses=0, wins='0', losses='0', trades='0', calcs=[]
                )
                continue

            stGroup.insert(0, 'interval', str(idx))
            stGroup.insert(0, 'amount', self.amount)

            winsGroup = stGroup[stGroup.delta > 0]
            lossesGroup = stGroup[stGroup.delta <= 0]

            fees = round(stGroup['fees'].sum(), 2)
            total = round(stGroup['delta'].sum(), 2)
            wins = round(winsGroup['delta'].sum(), 2)

            statistics[idx] = dict(
                fees=fees,
                profit=total-fees,
                total=total,
                totalWins=wins,
                totalLosses=total-wins,
                wins='%d' % winsGroup['delta'].count(),
                losses='%d' % lossesGroup['delta'].count(),
                trades='%d' % stGroup['delta'].count(),
                calcs=stGroup.to_dict('index')
            )

        del deals

        return statistics
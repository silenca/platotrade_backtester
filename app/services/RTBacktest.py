from datetime import datetime
from itertools import product
from multiprocessing.pool import Pool
from time import time

import numpy as np
import pytz
from dateutil.relativedelta import relativedelta
from pandas import to_datetime, DataFrame
from stockstats import StockDataFrame

from app.models.backtest import Backtest
from app.models.plato import Plato

from json import dumps

class RTBacktest():

    PERIOD_OFFSET = 33
    FRAME_SIZE = 40

    def __init__(self, plato_enter: Plato, plato_exit: Plato, rates: StockDataFrame, begin: int, end: int):
        self.plato_enter = plato_enter
        self.plato_exit = plato_exit

        self.rates = self.prepareRates(rates)
        self.begin = begin
        self.end = end

    def run(self):
        deals = DataFrame(columns=['ts_enter', 'ts_exit', 'price_enter', 'price_exit'])
        deal = None
        ts = time()
        i = 0

        #deals = DataFrame(columns=['key', 'ts_enter', 'ts_exit', 'price_enter', 'price_exit', 'opened'])

        for rawFrame in self.ratesGenerator():
            i += 1
            cur_ts = rawFrame.index.values[-1]

            if deal is None:
                advises = Calculator.calculateRealtimeAdvise(rawFrame, self.plato_enter)
                is_buy_advise = not advises[advises.advise == Plato.ADVISE_BUY].empty
                if is_buy_advise:
                    cur_ts = advises.index.values[-1]
                    deal = dict(
                        ts_enter=cur_ts,
                        price_enter=advises.close.values[-1]
                    )
                    #deals.append(DataFrame(deal, index=[len(deals)]), ignore_index=True)
            else:
                #deal_id = deals.index.values[-1]
                advises = Calculator.calculateRealtimeAdvise(rawFrame, self.plato_exit)
                is_sell_advise = not advises[advises.advise == Plato.ADVISE_SELL].empty
                if is_sell_advise:
                    cur_ts = advises.index.values[-1]

                    deal['ts_exit'] = cur_ts
                    deal['price_exit'] = advises.close.values[-1]

                    deals = deals.append(DataFrame(deal, index=[0]), ignore_index=True)
                    #print(deal, deals);exit(0)
                    deal = None

        stats = StatisticsCalc(self.end).calculate(deals);

        isPositive = False
        for period in stats:
            if stats[period]['total'] > 0:
                isPositive = True
                break

        if isPositive:
            return stats
        else:
            return None

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

    def prepareRates(self, rates: StockDataFrame):
        for key in rates.columns.values:
            if 'close_' in key:
                del rates[key]

        rates.minute_ts = rates.minute_ts.astype(int)
        rates.close = rates.close.astype(float)
        rates = rates.set_index('minute_ts')

        #rates.insert(len(rates.columns.values), 'time', to_datetime(rates.index, unit='s', utc=True))
        #rates['time'] = rates['time'].dt.strftime('%Y-%m-%d %H:%M:%S')

        return rates[['close']]

    def getPeriodSec(self, plato: Plato):
        return Calculator.getPeriodSec(plato)

class Calculator():
    FRAME_SIZE = 40

    @staticmethod
    def getPeriodSec(plato: Plato):
        return plato.period * 60

    @staticmethod
    def calculateRealtimeFrame(rawFrame: StockDataFrame, plato: Plato):
        periodFrame = rawFrame[rawFrame.index%Calculator.getPeriodSec(plato) == 0][-Calculator.FRAME_SIZE-1:]

        lastIdx = periodFrame.index.values[-1]
        nextIdx = lastIdx + Calculator.getPeriodSec(plato)

        frame = rawFrame[(rawFrame.index > lastIdx) & (rawFrame.index < nextIdx)]

        if not frame.empty:
            frame = frame[-1:]
            frame.index = [nextIdx]
            periodFrame = periodFrame.append(frame, verify_integrity=True)

        del frame
        del rawFrame

        return periodFrame

    @staticmethod
    def calculateRealtimeAdvise(rawFrame: StockDataFrame, plato: Plato):
        return plato.calculateReal(Calculator.calculateRealtimeFrame(rawFrame, plato), 1)


def itemsGenerator(rates: StockDataFrame, deals: DataFrame, pair: str='btc_usd'):
    for f1, s1, si1, p1, f2, s2, si2, p2 in product([12], [26], [9], [60], [12], [26], [9], [60]):
        pl1 = Plato(pair, f1, s1, si1, p1)
        pl2 = Plato(pair, f2, s2, si2, p2)

        key = '__'.join([pl1.key(), pl2.key()])

        yield (rates, pl1, pl2, deals[deals.key == key])

def processor(rates: StockDataFrame, enter_plato: Plato, exit_plato: Plato, deals: DataFrame):
    key = '__'.join([enter_plato.key(), exit_plato.key()])

    hasDeal = not deals[-1:][deals.opened == True].empty

    if not hasDeal:
        advises = Calculator.calculateRealtimeAdvise(rates, enter_plato)
        is_buy_advise = not advises[advises.advise == Plato.ADVISE_BUY].empty
        if is_buy_advise:
            cur_ts = advises.index.values[-1]
            deal = dict(
                key=key,
                ts_enter=cur_ts,
                price_enter=advises.close.values[-1],
                ts_exit=0,
                price_exit=0,
                opened=True
            )
            deals.append(DataFrame(deal, index=[len(deals)]), ignore_index=True)
    else:
        deal_id = deals.index.values[-1]
        advises = Calculator.calculateRealtimeAdvise(rates, exit_plato)
        is_sell_advise = not advises[advises.advise == Plato.ADVISE_SELL].empty
        if is_sell_advise:
            cur_ts = advises.index.values[-1]

            deals.loc[deal_id, 'ts_exit'] = cur_ts
            deals.loc[deal_id, 'price_exit'] = advises.close.values[-1]
            deals.loc[deal_id, 'opened'] = False

    del rates
    del enter_plato
    del exit_plato

    return deals

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
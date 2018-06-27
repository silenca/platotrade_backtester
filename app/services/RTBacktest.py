from datetime import datetime
from time import time

import pytz
from dateutil.relativedelta import relativedelta
from pandas import to_datetime, DataFrame
from stockstats import StockDataFrame

from app.models.plato import Plato

class RTBacktest():

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
        deals = DataFrame(columns=['ts_enter', 'ts_exit', 'price_enter', 'price_exit', 'emergency_enter', 'emergency_exit'])
        deal = None
        ts = time()
        i = 0

        last_price = None
        enter_advises_acc = None
        exit_advises_acc = None
        acc_capacity = 5;
        for rawFrame in generator:
            cur_ts = rawFrame.index.values[-1]
            if cur_ts < begin:
                del rawFrame
                continue
            i += 1
            last_price = (cur_ts, rawFrame.close.values[-1])
            # Calculate current intervals
            period_enter = Calculator.getPeriodSec(penter)
            period_exit = Calculator.getPeriodSec(pexit)
            cur_interval_enter = cur_ts - cur_ts%period_enter + period_enter
            cur_interval_exit = cur_ts - cur_ts%period_exit + period_exit
            # Calculate recent actions
            recently_opened = deal is not None \
                              and (cur_interval_enter - deal['ts_enter']) <= 2*period_enter
            recently_closed = deal is None \
                              and len(deals) \
                              and not deals.emergency_exit.values[-1] \
                              and (cur_interval_exit - deals.ts_exit.values[-1]) <= 2*period_exit

            enter_advises = Calculator.calculateRealtimeAdvise(rawFrame, penter)
            exit_advises = Calculator.calculateRealtimeAdvise(rawFrame, pexit)

            cur_enter_advise = enter_advises.advise.values[-1]
            cur_exit_advise = exit_advises.advise.values[-1]

            # Fill acc initial value
            if enter_advises_acc is None or enter_advises_acc[0] != cur_interval_enter:
                enter_advises_acc = (cur_interval_enter, [])
            if exit_advises_acc is None or exit_advises_acc[0] != cur_interval_exit:
                exit_advises_acc = (cur_interval_exit, [])
            # If there is enough data - skip first
            if len(enter_advises_acc[1]) == acc_capacity:
                enter_advises_acc = (enter_advises_acc[0], enter_advises_acc[1][1:])
            if len(exit_advises_acc[1]) == acc_capacity:
                exit_advises_acc = (exit_advises_acc[0], exit_advises_acc[1][1:])
            # Fill acc with current value
            enter_advises_acc[1].append(cur_enter_advise)
            exit_advises_acc[1].append(cur_exit_advise)

            is_buy_advise = False
            no_buy_advise = False
            if len(enter_advises_acc[1]) == acc_capacity:
                if all([a == Plato.ADVISE_BUY for a in enter_advises_acc[1]]):
                    is_buy_advise = True
                if all([a != Plato.ADVISE_BUY for a in enter_advises_acc[1]]):
                    no_buy_advise = True

            is_sell_advise = False
            no_sell_advise = False
            if len(exit_advises_acc[1]) == acc_capacity:
                if all([a == Plato.ADVISE_BUY for a in exit_advises_acc[1]]):
                    is_sell_advise = True
                if all([a != Plato.ADVISE_BUY for a in exit_advises_acc[1]]):
                    no_sell_advise = True

            if deal is None:
                is_emer = recently_closed and no_sell_advise
                if is_buy_advise or is_emer:
                    if len(deals[cur_interval_enter - deals.ts_enter <= period_enter]) <= 0:
                        deal = dict(
                            ts_enter=cur_ts,
                            price_enter=enter_advises.close.values[-1],
                            emergency_enter=is_emer,
                            emergency_exit=False
                        )
            else:
                is_emer = recently_opened and no_buy_advise
                if is_sell_advise or is_emer:
                    deal['ts_exit'] = cur_ts
                    deal['price_exit'] = exit_advises.close.values[-1]
                    deal['emergency_exit'] = is_emer

                    deals = deals.append(DataFrame(deal, index=[0]), ignore_index=True)
                    deal = None

        if deal is not None and last_price is not None:
            if deal['ts_enter'] != last_price[0]:
                deal['price_exit'] = last_price[1]
                deal['ts_exit'] = last_price[0]

                deals = deals.append(DataFrame(deal, index=[deal['ts_exit']]), ignore_index=True)

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

        if lts < nextIdx:
            frame = rawFrame[-1:]
            frame.index = [nextIdx]
            periodFrame = periodFrame.append(frame, verify_integrity=True)
            del frame

        del rawFrame

        return periodFrame

    @staticmethod
    def calculateRealtimeAdvise(rawFrame: StockDataFrame, plato: Plato, count: int=1):
        return plato.calculateReal(Calculator.calculateRealtimeFrame(rawFrame, plato), count)

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
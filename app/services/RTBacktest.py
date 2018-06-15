from datetime import datetime
from time import time

import numpy as np
import pytz
from dateutil.relativedelta import relativedelta
from pandas import to_datetime, DataFrame
from stockstats import StockDataFrame

from app.models.backtest import Backtest
from app.models.plato import Plato


class RTBacktest():

    PERIOD_OFFSET = 33
    FRAME_SIZE = 40

    def __init__(self, plato: Plato, rates: StockDataFrame, begin: int, end: int):
        self.plato = plato
        self.rates = self.prepareRates(rates)
        self.begin = begin
        self.end = end

    def run(self):
        deals = DataFrame(columns=['ts_enter', 'ts_exit', 'price_enter', 'price_exit'])
        deal = None
        ts = time()
        i = 0
        for sdf, curTs in self.ratesGenerator():
            i += 1
            advises = self.plato.calculateReal(sdf, 1)

            curAdvise = dict(
                ts=curTs,
                price=advises.close.values[-1],
                buy_s=not advises[advises.advise == Plato.ADVISE_BUY].empty,
                sell_s=not advises[advises.advise == Plato.ADVISE_SELL].empty,
                tmp=advises.advise.values
            )

            if deal is None:
                if curAdvise['buy_s']:
                    deal = dict(
                        ts_enter=curAdvise['ts'],
                        price_enter=curAdvise['price']
                    )
            else:
                if curAdvise['sell_s']:
                    deal['ts_exit'] = curAdvise['ts'],
                    deal['price_exit'] = curAdvise['price']

                    deals = deals.append(DataFrame(deal), ignore_index=True)
                    deal = None

        stats = StatisticsCalc(self.end).calculate(deals);
        print(deals)
        print(stats);
        print(f'Calculated {i} frames in {round(time()-ts, 3)}s')

        backtests = [dict(
            buy_fast=self.plato.fast,
            buy_slow=self.plato.slow,
            buy_signal=self.plato.signal,
            buy_period=self.plato.period,
            sell_fast=self.plato.fast,
            sell_slow=self.plato.slow,
            sell_signal=self.plato.signal,
            sell_period=self.plato.period,
            status='3',
            type='1',
            data=f'{dict(statistics=stats)}',
            extend='main.backtest',
            name=f'Buy: {self.plato.key()}, Sell: {self.plato.key()}',
            total_month6=float(stats['4']['profit']),
            total_month3=float(stats['3']['profit']),
            total_month1=float(stats['2']['profit']),
            total_week=float(stats['1']['profit']),
            ts_start=self.begin,
            ts_end=self.end
        )]

        Backtest.saveMany(backtests)

        return []

    def ratesGenerator(self):
        max = len(self.rates)
        max -= self.FRAME_SIZE*self.plato.period
        for begin in range(max):
            end = begin + self.FRAME_SIZE*self.plato.period
            yield self.calculateRealtimeFrame(self.rates[begin:end])

    def calculateRealtimeFrame(self, rawFrame: StockDataFrame):
        periodFrame = rawFrame[rawFrame.index%self.getPeriodSec() == 0][-self.FRAME_SIZE-1:]

        lastIdx = np.max(periodFrame.index.values)
        nextIdx = lastIdx + self.getPeriodSec()

        frame = rawFrame[(rawFrame.index > lastIdx) & (rawFrame.index < nextIdx)]

        if not frame.empty:
            frame = frame[-1:]
            frame.index = [nextIdx]
            periodFrame = periodFrame.append(frame, verify_integrity=True)

        curTs = rawFrame.index.values[-1]

        del frame
        del rawFrame

        return (periodFrame, curTs)

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

    def getPeriodSec(self):
        return 60*self.plato.period

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
                    feesAmount=0, totalAmount=0, winsAmount=0, lossesAmount=0,
                    wins='0', losses='0', total='0', calcs=[]
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
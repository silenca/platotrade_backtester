import logging
from json import dumps
from config import FileConfig
from time import time
from multiprocessing import Pool
from app.services.RateLoader import RateLoader
from app.models.backtest import Backtest
from app.bthelper import Backtest as BacktestHelper, Generator

class GlobalBacktest:
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
        print(msg)
        self.logger.info(msg)

    def run(self):
        ts = time()

        generator = Generator(self.pair, self.rateData, self.tsFrom, self.tsTo)

        poolConfig = dict(
            processes=FileConfig().get('APP.POOL_PROCESSES', 4, int),
            maxtasksperchild=FileConfig().get('APP.POOL_TASK_PER_CHILD', 10, int)
        )

        pool = Pool(**poolConfig)
        calculations = pool.starmap(BacktestHelper.calculate, generator.getItems())
        pool.close()
        pool.join()
        #calculations = []
        #for i in generator.getItems():
        #    calculations.append(BacktestHelper.calculate(*i))

        self.log('Pool calculation is done in '+'%.3fs'%(time()-ts))

        backtests = []
        self.log(f'There was {len(calculations)} items')
        for data in calculations:
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
                    ts_end=self.tsTo,
                    is_rt=0,
                    active=1
                ))
        self.log(f'Saving {len(backtests)} backtests');
        ts = time()
        Backtest.saveMany(backtests)
        self.log('Saved in %.4fs' % (time() - ts))
        self.log(f'There are {generator.countItems()} items and {(self.tsTo - self.tsFrom)//(24*60*60)} days')
        return generator.countItems();

    def smap(self, func, items):
        for item in items:
            return func(*item)


from app.services.RateLoader import RateLoader
from app.models.plato import Plato
from collections import deque
from itertools import product
from time import time
from multiprocessing import Pool
from functools import reduce
from operator import mul

class GlobalBacktest:

    FAST_PERIOD = range(2, 30)
    SLOW_PERIOD = range(10, 40)
    SIGNAL_PERIOD = range(2, 20)
    INTERVALS = [15, 30, 60, 120, 240, 1440]

    FEE = 0.002

    def __init__(self, pair, tsFrom, tsTo):
        ts = time()
        self.tsFrom = tsFrom
        self.tsTo = tsTo
        self.pair = pair

        self.rateData = RateLoader().fetchPeriods(self.pair, self.tsFrom, self.tsTo, self.INTERVALS)
        print(f'All data loaded in {time() - ts}s')

    def run(self):
        iterators = [self.FAST_PERIOD, self.SLOW_PERIOD, self.SIGNAL_PERIOD, self.INTERVALS]
        items = product(*iterators)

        pool = Pool(processes=8, maxtasksperchild=5)
        pool.starmap(self.calculate, self.getItems())
        pool.close()
        pool.join()

        return reduce(mul, map(len, iterators), 1);
        #return deque(map(self.calculate, items));

    def calculate(self, plato, stockData):
        ts = time()
        #period = productItem[3]
        #plato = Plato(self.pair, productItem[0], productItem[1], productItem[2], period)

        #stockData = self.rateData.getSdf(self.pair, period)
        plato.calculateAll(stockData)

        deals = self.getDeals(plato);

        totals = dict(fees=0, wins=0, losses=0, total=0)
        for deal in deals:
            if deal['delta'] > 0:
                totals['wins'] += deal['delta']
            else:
                totals['losses'] += abs(deal['delta'])
            totals['fees'] += deal['fees']
        totals['total'] = totals['wins'] - totals['losses']
        if totals['total'] > 0:
            print('**************', plato.key(), '**************')
            print(totals)
        else:
            print(plato.key())
        return (plato.key(), deals)

    def getDeals(self, plato):
        deals = []

        buy = None
        for date, advise in plato.advises.items():
            if buy is None:
                if advise['advise'] == Plato.ADVISE_BUY:
                    buy = advise
            elif advise['advise'] == Plato.ADVISE_SELL:
                capital = float(buy['close']);
                enterFee = capital*self.FEE
                capital -= enterFee
                exitCapital = float(advise['close'])
                exitFee = exitCapital*self.FEE
                exitCapital -= exitFee

                fees = enterFee + exitFee
                delta = exitCapital - capital - fees

                deals.append({
                    'enter': buy['close'],
                    'exit': advise['close'],
                    'enterTs': buy['date'],
                    'exitTs': advise['date'],
                    'delta': delta,
                    'fees': fees
                })
                buy = None

        return deals

    def getItems(self):
        iterators = [self.FAST_PERIOD, self.SLOW_PERIOD, self.SIGNAL_PERIOD, self.INTERVALS]

        for item in product(*iterators):
            plato = Plato(self.pair, item[0], item[1], item[2], item[3])
            stockData = self.rateData.getSdf(self.pair, item[3])
            yield (plato, stockData)
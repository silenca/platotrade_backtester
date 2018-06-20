from copy import deepcopy
from functools import reduce
from itertools import product
from time import time
from timeit import timeit

from dateutil import relativedelta
from multiprocessing import Pool

from pandas import DataFrame, Series

from app.models.RateData import RateData
from app.models.plato import Plato
from app.services.GlobalBacktest import GlobalBacktest
from app.services.RateLoader import RateLoader

PAIR_LIST = ['btc_usd']
DAY = 60*60*24

DEBUG = False

def run(pair, begin, end):
    print(f'Running {pair} from {begin} till {end} ...')

    GlobalBacktest(pair, begin, end).run()

if __name__ == '__main__':
    DEBUG = True
    end = int(time())
    begin = end - 180*DAY

    ts = time()
    run(PAIR_LIST[0], begin, end)

    print(f'Total: {time() - ts}')

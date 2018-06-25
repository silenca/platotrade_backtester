from os import listdir, utime, remove
from os.path import isfile, join, abspath, dirname

from app.models.backtest import Backtest
from app.models.plato import Plato
from app.services.RTBacktest import RTBacktest
from app.services.RateLoader import RateLoader
from app.bthelper import Backtest as BacktestHelper
from config import FileConfig
from json import dumps

basedir = abspath(dirname(__file__))

def files_list(dir):
    return [f for f in listdir(dir) if isfile(join(dir, f))]

def get_calculating():
    return list(map(int, files_list(join(basedir, 'btrun'))))

def touch(fname, times=None):
    fhandle = open(fname, 'a')
    try:
        utime(fname, times)
    finally:
        fhandle.close()

if __name__ == '__main__':
    calculating = get_calculating()
    max_running = FileConfig().get('APP.POOL_PROCESSES', 4, int)

    bt = None

    if len(calculating) < max_running:
        bt = Backtest.findOneToProcess(calculating)
    else:
        print('Server is busy now')
        exit(0)

    if bt is None:
        print('No backtest found')
        exit(0)

    fname = join(basedir, 'btrun', str(bt.id))

    touch(fname)

    penter = Plato('btc_usd', bt.buy_fast, bt.buy_slow, bt.buy_signal, bt.buy_period)
    pexit = Plato('btc_usd', bt.sell_fast, bt.sell_slow, bt.sell_signal, bt.sell_period)

    tsFrom = bt.ts_start
    tsTo = bt.ts_end

    if bt.is_rt:
        offset = max(penter.period, pexit.period) * 60 * 40

        rates = RateLoader().fetchPeriods('btc_usd', tsFrom - offset, tsTo, [1]).getSdf('btc_usd', 1)

        statistics = RTBacktest(penter, pexit, rates, tsFrom, tsTo).run()
    else:
        rates = RateLoader().fetch(dict(enter=penter, exit=pexit), tsFrom, tsTo)

        calculation = BacktestHelper.calculate(
            penter=penter, pexit=pexit,
            denter=rates.getSdf(penter.pair, penter.period),
            dexit=rates.getSdf(pexit.pair, pexit.period),
            begin=tsFrom, end=tsTo, force=True
        )

        _, _, _, _, statistics, _ = calculation

    remove(fname)

    bt.status = 3
    bt.data = f'{dumps(dict(statistics=statistics))}'
    bt.total_month6 = round(float(statistics['4']['profit']), 2)
    bt.total_month3 = round(float(statistics['3']['profit']), 2)
    bt.total_month1 = round(float(statistics['2']['profit']), 2)
    bt.total_week = round(float(statistics['1']['profit']), 2)

    bt.save()
    exit('Done')


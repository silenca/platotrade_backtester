from flask import request, jsonify
from flask_jsonpify import jsonpify

from app import app
from app.bthelper import Backtest as BacktestHelper
from app.models.backtest import Backtest
from app.services.GlobalBacktest import GlobalBacktest
from app.services.MacdDict import MacdDict
from app.services.RateLoader import RateLoader
from app.helper import setup_loggin
from app.models.plato import Plato

from time import time
from json import dumps

logger = setup_loggin()

# app = Flask(__name__)
# app.debug = True

@app.route('/plato/list', methods=['GET'])
def getAllPlatos():
    return jsonpify({p.key(): p.json() for p in MacdDict().getAll().values()})

@app.route('/plato/add', methods=['POST', 'GET'])#GET for DEBUG
def addPlato():
    for field in ['pair', 'fast_period', 'slow_period', 'signal_period', 'time_period']:
        if request.args.get(field) is None:
            return jsonpify({ 'message': f'Field "{field}" is required', 'status': '1' })

    pair = request.args.get('pair');
    fast = request.args.get('fast_period');
    slow = request.args.get('slow_period');
    signal = request.args.get('signal_period');
    time = request.args.get('time_period');

    plato = Plato(pair, fast, slow, signal, time)

    MacdDict().add(plato)

    return jsonpify(plato.json())

@app.route('/plato/remove/<string:key>', methods=['POST', 'GET'])# GET for DEBUG
def removePlato(key):
    MacdDict().remove(key);
    return jsonpify({'status': 0, 'message': 'Object has been deleted'})

@app.route('/plato/calculateall', methods=['GET'])
def calculation():
    platos = MacdDict().getAll();

    rateData = RateLoader().fetchLast(platos);

    for key in platos:
        plato = platos[key];

        stockData = rateData.getSdf(plato.pair, plato.period)

        plato.calculateLast(stockData)

    del rateData

    return getAllPlatos()

@app.route('/plato/calculate/<string:key>', methods=['GET'])
def calculationSingle(key):
    plato = MacdDict().get(key)

    if plato is None:
        return jsonpify({'message': 'No plato found', 'status': '1'})

    stockData = RateLoader()\
                    .fetchLast({0: plato})\
                        .getSdf(plato.pair, plato.period)

    plato.calculateLast(stockData)

    return jsonpify(plato.json())

@app.route('/backtest/run', methods=['POST', 'GET'])
def runBacktest():
    params = request.args

    for key in ['pair', 'from', 'to', 'coeffs[0]', 'coeffs[1]']:
        if params.get(key) is None:
            return jsonpify({'result': False, 'message': f'Param "{key}" is required'})

    pair = params['pair']
    tsFrom = int(params['from'])
    tsTo = int(params['to'])

    enter_params = list(map(int, params['coeffs[0]'].split('_')))
    penter = Plato(pair, *enter_params)

    exit_params = list(map(int, params['coeffs[1]'].split('_')))
    pexit = Plato(pair, *exit_params)

    rateData = RateLoader().fetch(dict(enter=penter, exit=pexit), tsFrom, tsTo)

    calculation = BacktestHelper.calculate(
        penter=penter, pexit=pexit,
        denter=rateData.getSdf(penter.pair, penter.period),
        dexit=rateData.getSdf(pexit.pair, pexit.period),
        begin=tsFrom, end=tsTo, force=True
    )

    _, _, _, _, statistics, _ = calculation

    bt = Backtest(
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
        ts_start=tsFrom,
        ts_end=tsTo,
        is_rt=0
    )

    bt.save()

    return jsonpify(dict(
        result=1,
        message='Backtest successfully calculated',
        backtest=dict(
            id=bt.id
        )
    ))

if __name__ == '__main__':
    app.run(debug=True) # Run app
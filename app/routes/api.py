from flask import request, jsonify
from flask_jsonpify import jsonpify
from numpy import place, random

from app import app
from app.services.RateLoader import RateLoader
from app.utils import fetch, get_macd_by_id, parse_data
from app.services.macd import MACD
from app.helper import setup_loggin
from app.services.backtester import backtest_all
from app.services.MacdDict import MacdDict
from app.models.plato import Plato
from random import randrange
from functools import reduce
from app.services.GlobalBacktest import GlobalBacktest
from time import time

logger = setup_loggin()

macd_objects = []
data = dict()

# app = Flask(__name__)
# app.debug = True

@app.route('/plato/list', methods=['GET'])
def getAllPlatos():
    platos = MacdDict().getAll()

    return jsonpify([platos[p].json() for p in platos])

@app.route('/plato/add', methods=['GET'])
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

@app.route('/plato/remove/<string:key>', methods=['POST', 'GET'])
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

@app.route('/plato/backtest/run', methods=['POST', 'GET'])
def runBakctest():
    params = request.args

    for key in ['pair', 'from', 'to', 'coeffs[0]', 'coeffs[1]']:
        if params.get(key) is None:
            return jsonpify({ 'result': False, 'message': f'Param "{key}" is required' })

    pair = params['pair']
    tsFrom = int(params['from'])
    tsTo = int(params['to'])

    platos = {}
    for coeff in [params['coeffs[0]'].split('_'), params['coeffs[1]'].split('_')]:
        plato = Plato(pair, coeff[0], coeff[1], coeff[2], coeff[3])
        platos[plato.key()] = plato

    rateData = RateLoader().fetch(platos, tsFrom, tsTo)

    for key in platos:
        stockData = rateData.getSdf(plato.pair, plato.period)

        plato.calculateAll(stockData)

    return jsonify([platos[key].json(True) for key in platos])


@app.route('/plato/backtest/runall', methods=['GET'])
def runGlobalBacktest():
    params = request.args
    for field in ['from', 'to', 'pair']:
        if params.get(field) is None:
            return jsonpify({ 'message': f'Param "{field}" is required', 'status': '1' })

    # Run all backtests
    ts = time()
    itemsCount = GlobalBacktest(params['pair'], int(params['from']), int(params['to'])).run()

    """
        Run calculate backtest for all combinations
        :query_param pair
        :query_param from
        :query_param to
        """

    #backtest_all(int(params['from']), int(params['to']), params['pair'])
    return jsonpify({ 'message': 'Done', 'status': '1', 'Total': time()-ts, 'Size': itemsCount })

if __name__ == '__main__':
    app.run(debug=True) # Run app
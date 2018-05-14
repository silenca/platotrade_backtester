from flask import Flask, request, jsonify
from flask_jsonpify import jsonpify

from app import app
from app.utils import fetch, get_macd_by_id, parse_data
from app.macd import MACD
from app.helper import setup_loggin
from app.bactester import backtest_all

logger = setup_loggin()

macd_objects = []
data = dict()

# app = Flask(__name__)
# app.debug = True


@app.route('/macd', methods=['GET'])
def get_all_macd_objects():
    """
    Return all MACD-objects

    :return: list of dict
    """

    global macd_objects


    return jsonpify([m.__dict__ for m in macd_objects])
    
@app.route('/calc', methods=['GET'])
def calcAll():
    """
    Calculate the new macd-coefficients for all MACD-objects

    :return: List of serialized MACD-objects
    """
    global macd_objects
    global data

    for macd in macd_objects:
        try:
            if macd.pair not in data:
                data[macd.pair] = fetch(macd.pair) # get data
                data[macd.pair] = parse_data(data[macd.pair]) # in each pair is stored sdf-data itself

        except Exception as err:
            return jsonpify(err)

        sdf = macd.calculate_coefficient(data[macd.pair][macd.time_period])
        sdf = macd.last_coefficient(sdf)

    data = dict() # empty data
    return jsonpify([m.__dict__ for m in macd_objects])

@app.route('/addplato', methods=['PUT'])
def addplato():
    """
    Create the new MACD-object and store it in 'macd_objects' list globally

    :query_param pair
    :query_param fast_period
    :query_param slow_period
    :query_param signal_period    
    :query_param time_period
    :query_param plato_ids

    :return: dict
    """

    params = request.args

    if MACD.paramsIsNotValid(params):
        return 'Error'

    global macd_objects  
    global data    

    if get_macd_by_id(params['plato_ids'], macd_objects) != None:
        return 'Object already exists'
    
    # request to Plato-microservice
    macd = MACD(params['pair'], params['fast_period'], params['slow_period'], params['signal_period'], params['time_period'], params['plato_ids'])
    
    macd_objects.append(macd)

    return jsonpify(macd.__dict__)

@app.route('/calc/<string:plato_ids>', methods=['PUT'])
def calc(plato_ids):
    """
    Calculate the new macd-coefficients for existing MACD-object by 'plato_ids'

    :param plato_id: Id of MACD-object
    :return: dict
    """
    
    global macd_objects

    macd = get_macd_by_id(plato_ids, macd_objects)
    if macd == None:
        return jsonpify({ 'message': 'Object is not exists', 'status': 1 })

    try:
        sdf = fetch(macd.pair, macd.time_period)
    except Exception as err:
        return jsonpify({ 'message': err, 'status': 1 })

    sdf = macd.calculate_coefficient(sdf)

    return jsonpify(macd.__dict__)


@app.route('/delete/macd/<string:plato_ids>', methods=['DELETE'])
def delete_macd_object(plato_ids):
    """
    Delete the MACD-object by 'plato_ids'

    :param plato_id: Id of MACD-object

    :return: None or dict
    """
    global macd_objects

    macd = get_macd_by_id(plato_ids, macd_objects)

    if macd != None:
        macd_objects.remove(macd)
        return jsonpify({ 'message': 'Object has been deleted', 'status': 0 })
    else:
        return jsonpify({ 'message': 'Object is not exists', 'status': 1 })


@app.route('/backtester', methods=['GET'])
def backtester():
    """
        Create the new MACD-object and calculated macd-coefficients for whole period

        :query_param pair
        :query_param fast_period
        :query_param slow_period
        :query_param signal_period
        :query_param period
        :query_param from
        :query_param to

        :return: dict
        """

    params = request.args
    macd_coeff = [params['coeffs[0]'].split('_'),
                  params['coeffs[1]'].split('_')]

    macds = []
    for coeff in macd_coeff:
        logger.info(f'start  - {coeff}')
        macd = MACD(params['pair'], coeff[0], coeff[1], coeff[2], coeff[3], plato_ids=None)
        data = macd.get_data(int(params['from']), int(params['to']))
        stock = macd.calculate_coefficient(data)
        stock = stock.set_index('minute_ts')
        macd.coefficients = stock[['macd', 'macdh', 'macds', 'close']].T.to_dict()
        macds.append(macd)
    return jsonify([macd.__dict__ for macd in macds])


@app.route('/startbacktest', methods=['GET'])
def start_backtest():
    """
        Run calculate backtest for all combinations
        :query_param pair
        :query_param from
        :query_param to
        """
    params = request.args
    backtest_all(int(params['from']), int(params['to']), params['pair'])


if __name__ == '__main__':
    app.run(debug=True) # Run app
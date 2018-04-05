from flask import Flask, request, jsonify
from flask_jsonpify import jsonpify
from flask_socketio import SocketIO, send, emit
import requests
import time
import stockstats
from threading import Lock

from .utils import fetch, get_macd_by_id, is_macd_object_exists, parse_data
from .macd import MACD

macd_objects = []
data = dict()

app = Flask(__name__)
app.debug = True


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

    print(macd_objects)
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


if __name__ == '__main__':
    app.run() # Run app
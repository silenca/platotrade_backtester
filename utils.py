import requests

import pandas as pd

import datetime
import time
from stockstats import StockDataFrame

def fetch(pair, time_period):
    """
    Fetch data from Plato-microservice by last 30 min
    :param time_period:

    :return: StockDataFrame
    """

    url = f'http://platotradeinfo.silencatech.com/main/dashboard/ajaxgetetradedata?pair={pair}'
    
    response = requests.get(url)
    return parse_data(response.json()['result'][time_period])

def parse_data(data):
    """
    Parse the response and retype the DataFrame object to StockDataFrame
    :param data: response from microservice

    :return: StockDataFrame
    """

    rows = []
    for obj in data:
        minute_ts = datetime.datetime.fromtimestamp(int(obj['minute_ts'])).strftime('%Y-%m-%d %H:%M:%S')
        v =  obj['v']
        l =  obj['l']
        h =  obj['h']
        c =  obj['c']
        vo = obj['vo']
        o =  obj['o']
        rows.append([minute_ts, vo, h, c, o, l, v])
    
    sdf = StockDataFrame.retype(
        pd.DataFrame(rows, columns=['date', 'volume', 'high', 'close', 'open', 'low', 'amount'])
    )

    return sdf

def get_macd_by_id(id, items):
    """
    Get item from list by id
    
    :param id:
    :param items: list of items

    :return dict or None
    """
    # for x in items:
    #     print(x)

    item = [x for x in items if x['plato_ids'] == int(id)]
    return item[0] if len(item) > 0 else None

def is_macd_object_exists(id, items):
    """
    Check if the object exists
    
    :param id:
    :param items: list of items

    :return: bool
    """
    macd = get_macd_by_id(id, items)
    return True if macd != None else False
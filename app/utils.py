import requests
import pandas as pd
import datetime

from stockstats import StockDataFrame


def fetch(pair, time_period=None, interval=None):
    """
    Fetch data from Plato-microservice by last 30 min
    :param time_period:

    :return: json
    """

    if time_period is None:
        url = f'http://platotradeinfo.silencatech.com/main/dashboard/ajaxgetetradedata'
        response = requests.get(url, params={'pair': pair})
        return response.json()['result']
    elif time_period is not None and interval is not None:
        url = 'http://platotradeinfo.silencatech.com/main/dashboard/ajaxgetetradedataforperiod'
        response = requests.get(url, params={'pair': pair,
                                             'from': time_period['from'],
                                             'to': time_period['to'],
                                             'period': interval})
        return response.json()['data']


def parse_date_period(data):
    df = pd.DataFrame(data)
    df = df[['minute_ts', 'o',  'h', 'l', 'c', 'vo']]
    df = df.sort_values(by=['minute_ts'])
    df = df.rename(columns={'vo': 'volume',
                            'o': 'open',
                            'h': 'high',
                            'l': 'low',
                            'c': 'close'})

    date = pd.to_datetime(df['minute_ts'], unit='s')
    df.insert(0, 'date', date)
    df['date'] = df['date'].dt.strftime('%Y-%m-%d %H:%M:%S')

    return StockDataFrame.retype(df)

def parse_data(data):
    """
    Parse the response and retype the DataFrame object to StockDataFrame
    :param data: response from microservice

    :return: StockDataFrame
    """

    d = dict()
    for key in data.keys():
        rows = []
        for obj in reversed(data[key]):
            minute_ts = datetime.datetime.fromtimestamp(int(obj['minute_ts'])).strftime('%Y-%m-%d %H:%M:%S')
            ts = int(obj['minute_ts'])
            v = obj['v']
            l = obj['l']
            h = obj['h']
            c = obj['c']
            vo = obj['vo']
            o = obj['o']
            rows.append([minute_ts, ts, vo, h, c, o, l, v])
        
        sdf = StockDataFrame.retype(
            pd.DataFrame(rows, columns=['date', 'ts', 'volume', 'high', 'close', 'open', 'low', 'amount'])
        )

        d[key] = sdf

    return d


def get_macd_by_id(id, items):
    """
    Get item from list by id
    
    :param id:
    :param items: list of items

    :return MACD or None
    """

    for x in items:
        if x.plato_ids == id:
            return x
        
    return None


def is_macd_object_exists(id, items):
    """
    Check if the object exists
    
    :param id:
    :param items: list of items

    :return: bool
    """
    macd = get_macd_by_id(id, items)
    return True if macd != None else False
import requests
import pytz
import pytest
from datetime import datetime

from app.macd import MACD

macd = MACD('btc_usd', 12, 26, 9, 60, '1')


def convert_to_tmsp(date):
    utc = pytz.timezone('UTC')
    dt = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
    return utc.localize(dt).timestamp()


def test_skip_data():
    from_ = '2018-05-01 00:00:00'
    to_ = '2018-05-07 00:00:00'
    data = macd.get_data(convert_to_tmsp(from_), convert_to_tmsp(to_))
    assert data['minute_ts'][0] == from_


def test_backtest():
    r = requests.get('http://127.0.0.1/backtester?pair=BTC_USD&from=1525608000&to=1525694400&coeffs[0]=12_26_9_120&coeffs[1]=12_26_9_30')
    print(r.json())
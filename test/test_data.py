import pytz
import pytest
from datetime import datetime

from app.macd import MACD
from app.utils import fetch

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


def test_cache_platotradeinfo():
    macd.get_data(1519225472, 1619225472)

intervals = [1, 2, 5, 10, 60, 120, 240, 1440, 10080]

@pytest.mark.parametrize('interval', intervals)
def test_fetch(interval):
    assert fetch("BTC_USD", interval=interval, time_period={'from': 1514764800, 'to': 1526893778})

import requests
import datetime
import pandas as pd
from stockstats import StockDataFrame


class InvestingCom:
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 '
                      'Safari/537.36'}

    def get_data(self, to_time, duration=15):
        count_rows = 40
        from_date = to_time - (count_rows * duration * 60)
        params = {'symbol': 49798,
                  'resolution': duration,
                  'from': from_date,
                  'to': to_time}

        r = requests.get(
            'https://tvc4.forexpros.com/1e8507a55858eeadbfe47be9c57387b3/1518710727/1/1/8/history', params=params,
            headers=self.headers)
        tr_data = r.json()
        data = pd.DataFrame()
        data['date'] = list(
            map(lambda x: datetime.datetime.fromtimestamp(int(x)).strftime('%Y-%m-%d %H:%M:%S'), tr_data['t']))
        data['ts'] = tr_data['t']
        data['volume'] = tr_data['vo']
        data['high'] = tr_data['h']
        data['close'] = tr_data['c']
        data['open'] = tr_data['o']
        data['low'] = tr_data['l']
        data['amount'] = tr_data['v']
        sdf = StockDataFrame.retype(data)
        return sdf


    def current_rate(self):
        url = 'https://tvc4.forexpros.com/16d0366c5c1f829ac8499cf834a98a66/1520187160/7/7/18/quotes?symbols=Bitfinex%20%3ABTC%2FUS'
        r = requests.get(url,headers=self.headers).json()
        return r['d'][0]['v']

if __name__ == '__main__':
    now = int(datetime.datetime.now().timestamp())
    trade = InvestingCom()
    print(trade.get_data(now))

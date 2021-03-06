from app.services.adviser import calc_advise
from app.utils import fetch, parse_date_period
from app.helper import setup_loggin

logger = setup_loggin()

cache_platradeinfo = dict()


class MACD:

    skip_data = 34
    cache_platotradeinfo = dict()

    def __init__(self, pair, fast_period, slow_period, signal_period, time_period, plato_ids, coefs={}, macd=None,
                 macds=None, macdh=None):
        self.pair = pair
        self.fast_period = int(fast_period)
        self.slow_period = int(slow_period)
        self.signal_period = int(signal_period)
        self.time_period = int(time_period)
        self.plato_ids = plato_ids
        self.coefficients = coefs

    @staticmethod
    def paramsIsNotValid(params):
        try:
            if not params['pair'] or not params['fast_period'] or not params['slow_period'] or not params[
                'signal_period'] or not params['time_period'] or not params['plato_ids']:
                return True
        except:
            return True

        return False

    def calculate_coefficient(self, df):
        fast = df[f'close_{self.fast_period}_ema']
        slow = df[f'close_{self.slow_period}_ema']
        df['macd'] = fast - slow
        df['macds'] = df[f'macd_{self.signal_period}_ema']
        df['macdh'] = 2 * (df['macd'] - df['macds'])

        del df[f'macd_{self.signal_period}_ema']
        del fast
        del slow
        self.coefficients = df[['macd']].to_dict()
        return df[(self.skip_data - 1):]

    def last_coefficient(self, df):
        self.coefficients = {}

        df['advise'] = calc_advise(df)
        # self.timestamp = str(df.iloc[-1]['ts'])

        self.coefficients = df[-4:].to_dict('index')

    def get_data(self, _from, _to):
        _from = _from - self.time_period * 60 * self.skip_data
        key = f'{self.time_period}_{_from}'
        if cache_platradeinfo.get(key) is None:
            data = fetch(self.pair, interval=self.time_period, time_period={'from': _from, 'to': _to})
            cache_platradeinfo[key] = parse_date_period(data)

        return cache_platradeinfo.get(key)

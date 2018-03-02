import stockstats
import time

class MACD():

    def __init__(self, pair, fast_period, slow_period, signal_period, time_period, plato_ids, coefs={}, macd=None, macds=None, macdh=None):
        self.pair = pair
        self.fast_period = int(fast_period)
        self.slow_period = int(slow_period)
        self.signal_period = int(signal_period)
        self.time_period = time_period
        self.plato_ids = plato_ids
        self.coefficients = coefs

    @staticmethod
    def paramsIsNotValid(params):
        try:
            if not params['pair'] or not params['fast_period'] or not params['slow_period'] or not params['signal_period'] or not params['time_period'] or not params['plato_ids']:
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

        self.coefficients = {}

        self.coefficients[str(df.iloc[-2]['ts'])] = {
            'macd':  df.iloc[-2]['macd'],
            'macdh': df.iloc[-2]['macdh'],
            'macds': df.iloc[-2]['macds'],
        }

        self.coefficients[str(df.iloc[-1]['ts'])] = {
            'macd':  df.iloc[-1]['macd'],
            'macdh': df.iloc[-1]['macdh'],
            'macds': df.iloc[-1]['macds'],
        }

        # self.timestamp = str(df.iloc[-1]['ts'])

        return df


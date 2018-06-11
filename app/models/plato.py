from stockstats import StockDataFrame
import datetime

class Plato:

    SKIP_COUNT = 34

    ADVISE_SELL = 'Sell'
    ADVISE_BUY = 'Buy'
    ADVISE_NONE = 'No'

    def __init__(self, pair, fast, slow, signal, period):
        self.pair = pair
        self.fast = int(fast)
        self.slow = int(slow)
        self.signal = int(signal)
        self.period = int(period)
        self.coefficients = {}
        self.advises = {}

    def key(self, separator: str="_"):
        return separator.join(map(str, [self.fast, self.slow, self.signal, self.period]))

    def json(self, skipEmptyAdvises:bool = False):
        advises = {}
        for ts in self.advises:
            if not skipEmptyAdvises or self.advises[ts]['advise'] != self.ADVISE_NONE:
                advises[ts] = self.advises[ts]

        return {
            'pair': self.pair,
            'fast_period': self.fast,
            'slow_period': self.slow,
            'signal_period': self.signal,
            'time_period': self.period,
            #'coefficients': self.coefficients,
            'advises': advises,
            'plato_ids': self.key("0"), # Deprecated
            'key': self.key()
        };

    def calculateLast(self, stockData:StockDataFrame):
        stockData = self.__prepareData(stockData)

        self.__updateCoefficients(stockData[-4:])

    def calculateAll(self, stockData:StockDataFrame):
        stockData = self.__prepareData(stockData)

        self.__updateCoefficients(stockData[(self.SKIP_COUNT - 1):])

    def calculateAdvises(self):
        self.advises = {};

        lastCoeff = None
        for date in self.coefficients:
            coeff = self.coefficients[date]

            if lastCoeff is not None:
                if coeff['macdh'] > 0 and lastCoeff['macdh'] < 0:
                    type = self.ADVISE_BUY
                elif coeff['macdh'] < 0 and lastCoeff['macdh'] > 0:
                    type = self.ADVISE_SELL
                else:
                    type = self.ADVISE_NONE

                self.advises[date] = {
                    'ts': date,
                    'date': datetime.datetime.fromtimestamp(int(date)).strftime('%Y-%m-%d %H:%M:%S'),
                    'advise': type,
                    'close': coeff['close']
                }

            lastCoeff = coeff


    def __prepareData(self, stockData:StockDataFrame) -> StockDataFrame:
        fast = stockData[f'close_{self.fast}_ema']
        slow = stockData[f'close_{self.slow}_ema']

        stockData['macd'] = fast - slow
        stockData['macds'] = stockData[f'macd_{self.signal}_ema']
        stockData['macdh'] = 2 * (stockData['macd'] - stockData['macds'])

        return stockData

    def __updateCoefficients(self, stockData:StockDataFrame):
        coeffs = stockData.set_index('minute_ts')

        self.coefficients = coeffs[['macd', 'macds', 'macdh', 'close']].to_dict('index')
        self.calculateAdvises()
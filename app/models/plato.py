import numpy as np
from stockstats import StockDataFrame

class Plato():

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
        self.advises = {}
        self.adviseData = None

    def __repr__(self):
        return self.json()

    def key(self, separator: str="_"):
        return separator.join(map(str, [self.fast, self.slow, self.signal, self.period]))

    def json(self):
        return dict(
            pair=self.pair,
            fast_period=self.fast,
            slow_period=self.slow,
            signal_period=self.signal,
            time_period=self.period,
            advises=self.advises,
            key=self.key()
        );

    def calculateLast(self, stockData:StockDataFrame):
        stockData = self.__prepareData(stockData)

        self.__calculateAdvises(stockData[-3:])

    def calculateReal(self, sdf: StockDataFrame, lastrows: int=3) -> StockDataFrame:
        return self.__prepareData(sdf)[-lastrows:][['close', 'advise']]

    def calculateAll(self, stockData:StockDataFrame):
        stockData = self.__prepareData(stockData)

        self.__calculateAdvises(stockData[(self.SKIP_COUNT - 1):], False)

    def __prepareData(self, stockData:StockDataFrame) -> StockDataFrame:
        stockData['macd'] = stockData[f'close_{self.fast}_ema'].values - stockData[f'close_{self.slow}_ema'].values

        stockData['macds'] = stockData[f'macd_{self.signal}_ema'].values
        stockData['macdh'] = 2 * (stockData['macd'].values - stockData['macds'].values)

        # Generating Advises based on macdh
        stockData['pos'] = np.where(stockData.macdh > 0, 1, 0)
        stockData['prevPos'] = np.where(stockData.macdh.shift(1) > 0, 1, 0)
        stockData['advise'] = np.where(stockData.pos - stockData.prevPos < 0, self.ADVISE_SELL,
                               np.where(stockData.pos - stockData.prevPos > 0, self.ADVISE_BUY, self.ADVISE_NONE))
        #idx = stockData.index.values[-1]
        #if idx <= 1529042400 and idx >= 1529038800:
        #    print(stockData.index.values[-1], stockData[-3:])

        del stockData['pos']
        del stockData['prevPos']

        return stockData

    def __calculateAdvises(self, stockData:StockDataFrame, save: bool=True):
        self.adviseData = stockData[['minute_ts', 'close', 'advise']]
        if save:
            self.advises = stockData[['minute_ts', 'close', 'advise']].to_dict('index')

from pandas import DataFrame, to_datetime
from stockstats import StockDataFrame

class RateData:

    def __init__(self):
        self.data = {}
        self.sdfCache = {}

    def has(self, pair, period):
        period = int(period)
        if pair not in self.data:
            return False
        if period not in self.data[pair]:
            return False
        return True

    def add(self, pair, period, item):
        period = int(period)
        if pair not in self.data:
            self.data[pair] = {}

        if period not in self.data[pair]:
            self.data[pair][period] = {}

        self.data[pair][period][item['minute_ts']] = item

        return self;

    def load(self, pair, period, items):
        for item in items:
            self.add(pair, period, item)

    def get(self, pair, period):
        period = int(period)
        if pair not in self.data:
            return []
        if period not in self.data[pair]:
            return []

        return list(self.data[pair][period].values())

    def getSdf(self, pair, period) -> StockDataFrame:
        key = "_".join(map(str, [pair, period]))
        if key not in self.sdfCache:
            dataFrame = DataFrame(self.get(pair, period), columns=['minute_ts', 'o', 'h', 'l', 'c', 'vo'])

            dataFrame = dataFrame.sort_values(by=['minute_ts'])
            dataFrame = dataFrame.rename(columns={'vo': 'volume',
                                                    'o': 'open',
                                                    'h': 'high',
                                                    'l': 'low',
                                                    'c': 'close'})

            date = to_datetime(dataFrame['minute_ts'], unit='s')

            dataFrame.insert(0, 'date', date)
            dataFrame['date'] = dataFrame['date'].dt.strftime('%Y-%m-%d %H:%M:%S')

            self.sdfCache[key] = StockDataFrame.retype(dataFrame)

        return self.sdfCache[key]
import datetime

from macd import MACD
from .investing_com import InvestingCom

trade = InvestingCom()


class MACDAdvise(MACD):

    def __init__(self, pair, fast_period, slow_period, signal_period, time_period, plato_ids, coefs={}, macd=None, macds=None, macdh=None):
        super(MACDAdvise, self).__init__(pair, fast_period, slow_period, signal_period, time_period, plato_ids, coefs={}, macd=None, macds=None, macdh=None)
        self.last_calculation = None

    def handling_coefficient(self):
        now = int(datetime.datetime.now().timestamp())

        data = trade.get_data(now, duration=self.time_period)
        stock = self.calculate_coefficient(data)
        signal = stock['macds']  # Your signal line
        macd = stock['macd']  # The MACD that need to cross the signal line
        #                                              to give you a Buy/Sell signal
        listLongShort = ["No data"]  # Since you need at least two days in the for loop
        for i in range(1, len(signal)):
            # If the MACD crosses the signal line upward
            if macd.iloc[i] > signal.iloc[i] and macd.iloc[i - 1] <= signal.iloc[i - 1]:
                listLongShort.append("BUY")
            # The other way around
            elif macd.iloc[i] < signal.iloc[i] and macd.iloc[i - 1] >= signal.iloc[i - 1]:
                listLongShort.append("SELL")
            # Do nothing if not crossed
            else:
                listLongShort.append("HOLD")

        stock['advice'] = listLongShort
        # The advice column means "Buy/Sell/Hold" at the end of this day or
        #  at the beginning of the next day, since the market will be closed

        # print(stock.head('Advise')=listLo)
        data['advisor'] = listLongShort
        self.last_calculation = stock.tail(1)
        return stock


if __name__ == '__main__':
    macd = MACDAdvise('btc_usd', 12, 26, 9, 15, "1")
    macd.handling_coefficient()
    print((macd.last_calculation['advisor'] == 'HOLD').bool())

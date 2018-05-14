import pandas as pd

from app.helper import setup_loggin
from app.macd import MACD
from app.utils import fetch, parse_date_period
from test.adviser import calc_advise

logger = setup_loggin(filename='backtester.log')

INTERVALS = [15, 30, 60, 120, 240, 1440]
cache_data = {}


def backtest_all(from_, to_, pair):
    for period in INTERVALS:
        data = fetch(pair,time_period={'from': from_,'to':to_}, interval=period)
        cache_data[period] = parse_date_period(data)

    for fast_period in range(2, 30):
        for slow_period in range(10, 40):
            for signal_period in range(2, 20):
                for interval in INTERVALS:

                    macd = MACD(pair, fast_period, slow_period, signal_period, interval, plato_ids=None)
                    data = cache_data[interval]
                    stock = macd.calculate_coefficient(data)
                    calc_advise(stock)
                    backtest = Backtest(stock, stock)
                    backtest.calc_trades()
                    statistics = backtest.get_statistics()
                    print(statistics)
                    logger.info(f'params {fast_period}-{slow_period}-{signal_period}-{interval} -- {statistics}')

class Backtest:
    fast_range = {}
    slow_range = {}
    signal_range = {}
    time_range = {}
    COMMISSION = 0.002

    def __init__(self, buy_data, sell_data):
        self.buy = buy_data
        self.sell = sell_data
        self.capital = None
        self.lot_size = 1
        self.trades = []

    def calc_trades(self):
        buy = self.buy.loc[self.buy['advise'].isin(['BUY', "SELL"])]
        sell = self.sell.loc[self.sell['advise'].isin(["SELL"])]
        signals = pd.concat([buy, sell]).sort_values(['minute_ts'])
        wait_signal = 'BUY'

        for index, row in signals.iterrows():
            if 'BUY' == wait_signal and row['advise'] == 'BUY':
                if self.capital is None:
                    self.capital = float(row.get('close'))
                self.lot_size = (1 - self.COMMISSION) * (self.capital / float(row.get('close')))
                self.trades.append({'price_enter': row.get('close'),
                                    'ts_enter': row.get('minute_ts'),
                                    'capital': self.capital,
                                    'amount': self.lot_size,
                                    'fee_enter': self.capital * self.COMMISSION})
                wait_signal = 'SELL'

            elif 'SELL' == wait_signal and row['advise'] == wait_signal:
                income = self.lot_size * float(row.get('close'))
                fee_exit = income * self.COMMISSION
                income -= fee_exit

                self.trades[-1].update({'price_exit': float(row.get('close')),
                                        'delta': income - self.trades[-1]['capital'],
                                        'ts_exit': row.get('minute_ts'),
                                        'fee_exit': fee_exit,
                                        })
                wait_signal = 'BUY'
                self.capital = income
            else:
                pass
        if wait_signal == 'SELL':
            self.trades.pop(-1)
        return self.trades

    def get_statistics(self):
        wins = 0
        losses = 0
        total = 0
        total_wins = 0
        total_losses = 0
        fee = 0
        for trade in self.trades:
            total += trade['delta']
            fee += (trade['fee_enter'] + trade['fee_exit'])
            if trade['delta'] > 0:
                wins += 1
                total_wins += trade['delta']
            else:
                losses += 1
                total_losses += trade['delta']
        return {'wins': wins,
                'losses': losses,
                'total': total,
                'total_wins': total_wins,
                'total_losses': total_losses,
                'fees': fee}


if __name__ == '__main__':
    backtest_all(1525132800, 1526299200, 'BTC_USD')

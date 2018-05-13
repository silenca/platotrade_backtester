
class Backtest:

    fast_range = {}
    slow_range = {}
    signal_range = {}
    time_range = {}
    COMMISSION = 0.002

    def __init__(self, buy_data, sell_data):
        self.buy = buy_data
        self.sell = sell_data
        self.trades = []
        self.capital = None
        self.lot_size = 1

    def calc_trades(self):
        buy = self.buy.loc[self.buy['advise'].isin('BUY', "SELL")]
        sell = self.sell.loc[self.buy['advise'].isin("SELL")]
        signals = buy.union(sell).sort(['minute_ts'])
        wait_signal = 'BUY'

        for index, row in signals.iterows():
            if 'BUY' == wait_signal and row['advise'] == 'BUY':
                if self.capital is None:
                    self.capital = row.get('close')
                self.lot_size = (1 - self.COMMISSION) * (self.capital / row.get('close'))
                self.trades.append({'price_enter': row.get('close'),
                                    'ts_enter': row.get('minute_ts'),
                                    'capital': self.capital,
                                    'amount': self.lot_size,
                                    'fee_enter': self.capital * self.COMMISSION})
                wait_signal = 'SELL'

            elif 'SELL' == wait_signal and row['advise'] == wait_signal:
                income = self.lot_size * row.get('close')
                fee_exit = income * self.COMMISSION
                income -= fee_exit

                self.trades[-1] = {'price_exit': row.get('close'),
                                   'delta': income - self.trades[-1]['capital'],
                                   'ts_exit': row.get('minute_ts'),
                                   'fee_exit': fee_exit,
                                   }
                wait_signal = 'BUY'
                self.capital = income
            else:
                pass
        return self.trades

    # TODO create statistics
    def get_statistics(self):
        pass

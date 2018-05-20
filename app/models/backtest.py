from app import db


class Backtest(db.Model):

    __tablename__ = 'main_backtest'
    __table_args__ = {'extend_existing': True}

    id = db.Column(db.Integer, primary_key=True)

    buy_fast = db.Column(db.Integer)
    buy_slow = db.Column(db.Integer)
    buy_signal = db.Column(db.Integer)
    buy_period = db.Column(db.Integer)

    sell_fast = db.Column(db.Integer)
    sell_slow = db.Column(db.Integer)
    sell_signal = db.Column(db.Integer)
    sell_period = db.Column(db.Integer)

    ts_start = db.Column(db.Integer)
    ts_end = db.Column(db.Integer)

    status = db.Column(db.Integer)
    data = db.Column(db.Text)
    extend = db.Column(db.Text)
    name = db.Column(db.Text)
    total_month6 = db.Column(db.Float)

    @staticmethod
    def new_backtest(macd_params_buy,
                     macd_params_sell,
                     statistics,
                     start,
                     end):

        _backtest = Backtest(buy_fast=macd_params_buy[0],
                             buy_slow=macd_params_buy[1],
                             buy_signal=macd_params_buy[2],
                             buy_period=macd_params_buy[3],
                             sell_fast=macd_params_sell[0],
                             sell_slow=macd_params_sell[1],
                             sell_signal=macd_params_sell[2],
                             sell_period=macd_params_sell[3],
                             status=3,
                             data=f'{statistics}',
                             extend = 'main.backtest',
                             name=f'Buy: {macd_params_buy}, Sell: {macd_params_sell}',
                             total_month6=statistics['total'],
                             ts_start = start,
                             ts_end = end
                             )

        db.session.add(_backtest)
        db.session.commit()


if __name__ == '__main__':

    Backtest.new_backtest([0,0,0,0], [0,0,0,0], 'test')



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
    total_month3 = db.Column(db.Float)
    total_month1 = db.Column(db.Float)
    total_week = db.Column(db.Float)

    type = db.Column(db.Integer)
    is_rt = db.Column(db.Integer)

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
                             extend='main.backtest',
                             name=f'Buy: {macd_params_buy}, Sell: {macd_params_sell}',
                             total_month6=statistics['4']['total'],
                             total_month3=statistics['3']['total'],
                             total_month1=statistics['2']['total'],
                             total_week=statistics['1']['total'],
                             ts_start=start,
                             ts_end=end
                             )

        db.session.add(_backtest)
        db.session.commit()

    @staticmethod
    def saveMany(backtests):
        for params in backtests:
            if params is not None:
                filters = [
                    Backtest.buy_fast == params['buy_fast'],
                    Backtest.buy_slow == params['buy_slow'],
                    Backtest.buy_signal == params['buy_signal'],
                    Backtest.buy_period == params['buy_period'],
                    Backtest.sell_fast == params['sell_fast'],
                    Backtest.sell_slow == params['sell_slow'],
                    Backtest.sell_signal == params['sell_signal'],
                    Backtest.sell_period == params['sell_period'],
                    Backtest.is_rt == params['is_rt'],
                ]
                existing = db.session.query(Backtest).filter(*filters).first()
                if existing is None: # Create new entity
                    db.session.add(Backtest(**params))
                else: # Updating existing
                    existing.data = params['data']
                    existing.total_month6 = params['total_month6']
                    existing.total_month3 = params['total_month3']
                    existing.total_month1 = params['total_month1']
                    existing.total_week = params['total_week']
                    existing.ts_start = params['ts_start']
                    existing.ts_end = params['ts_end']

        db.session.commit()

    def save(self):
        db.session.add(self)
        db.session.commit()

    @staticmethod
    def find(id: int):
        return db.session.query(Backtest).get(id)

if __name__ == '__main__':

    Backtest.new_backtest([0,0,0,0], [0,0,0,0], 'test')



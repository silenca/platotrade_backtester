from app.models.RateData import RateData
import requests

from app.models.plato import Plato


class RateLoader:
    LAST_URL = 'http://platotradeinfo.silencatech.com/main/dashboard/ajaxgetetradedata'
    PERIOD_URL = 'http://platotradeinfo.silencatech.com/main/dashboard/ajaxgetetradedataforperiod'

    def fetchLast(self, platos) -> RateData:
        data = RateData();

        for key in platos:
            plato = platos[key];
            if not data.has(plato.pair, plato.period):
                rawResponse = requests.get(self.LAST_URL, params={'pair': plato.pair})
                response = rawResponse.json();
                if False == response['result']:
                    raise Exception('Error quering rates data')

                dataByPeriod = response['result']

                for period in dataByPeriod:
                    data.load(plato.pair, period, dataByPeriod[period])

        return data

    def fetch(self, platos, tsFrom:int, tsTo:int) -> RateData:
        data = RateData();

        for key in platos:
            plato = platos[key];
            if not data.has(plato.pair, plato.period):
                rates = self.__fetchByPairTsAndPeriod(plato.pair, tsFrom, tsTo, plato.period)

                data.load(plato.pair, plato.period, rates)

        return data

    def __fetchByPairTsAndPeriod(self, pair:str, tsFrom:int, tsTo:int, period:int) -> list:
        rawResponse = requests.get(self.PERIOD_URL, params={
            'pair': pair,
            'from': tsFrom,
            'to': tsTo,
            'period': period
        })
        response = rawResponse.json();
        if False == response['result']:
            raise Exception('Error quering rates data')

        return response['data']

    def fetchPeriods(self, pair:str, tsFrom:int, tsTo:int, periods:list) -> RateData:
        data = RateData()

        for period in periods:
            offset = Plato.SKIP_COUNT*period*60
            rates = self.__fetchByPairTsAndPeriod(pair, tsFrom-offset, tsTo, period)

            data.load(pair, period, rates)

        return data


import requests

server = 'http://localhost:5000'

plato = {'pair': 'BTC_USD',
         'fast_period': 12,
         'slow_period': 26,
         'signal_period': 9,
         'time_period': 15,
         'plato_ids': 1202609015}

# requests.put(f'{server}/addplato', params=plato)

print(requests.get(f'{server}/calc').json())
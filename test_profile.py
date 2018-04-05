from utils import fetch, get_macd_by_id, is_macd_object_exists, parse_data
from macd import MACD


macd_objects = []
data = dict()

def calcAll():

    global macd_objects
    global data

    for macd in macd_objects:
        try:
            if macd.pair not in data:
                data[macd.pair] = fetch(macd.pair) # get data                

        except Exception as err:
            # return print(err)
            pass

        sdf = parse_data(data[macd.pair][macd.time_period])
        sdf = macd.calculate_coefficient(sdf)

    # return jsonpify(data)
    data = dict() # empty data
    [m.__dict__ for m in macd_objects]
    

def addplato(args):
    """
    Create the new MACD-object and store it in 'macd_objects' list globally

    :query_param pair
    :query_param fast_period
    :query_param slow_period
    :query_param signal_period    
    :query_param time_period
    :query_param plato_ids

    :return: dict
    """

    params = args

    if MACD.paramsIsNotValid(params):
        return 'Error'

    global macd_objects  
    global data    

    if get_macd_by_id(params['plato_ids'], macd_objects) != None:
        return 'Object already exists'
    
    # request to Plato-microservice
    macd = MACD(params['pair'], params['fast_period'], params['slow_period'], params['signal_period'], params['time_period'], params['plato_ids'])
    
    macd_objects.append(macd)

    # print(macd_objects)
    # return jsonpify(macd.__dict__)

if __name__ == '__main__':

    args = {
      'pair': 'btc_usd',
      'fast_period': '12',
      'slow_period': '26',
      'signal_period': '9',
      'time_period': '5',
      'plato_ids': 'btc_usd_12_26_9_5'
    }


    for p in ['1', '5', '15', '30']:
      for f in range(8,12):
        for s in range(22, 30):
          for sp in range(8, 14):
            args['fast_period'] = f
            args['slow_period'] = s
            args['signal_period'] = sp
            args['time_period'] = p
            args['plato_ids'] = 'btc_usd_{}_{}_{}_{}'.format(f, s, sp, p)
            addplato(args)

    calcAll()
    # calcAll()

    print(len(macd_objects))
    # pair=btc_usd&fast_period=12&slow_period=26&signal_period=9&time_period=5&plato_ids=btc_usd_12_26_9_5
import json

import websocket

try:
    import thread
except ImportError:
    import _thread as thread
import time

chart = {'port': 8184, 'subscribe': lambda x: {"event": "subscribe", "period": x}}
rate = {'port': 8186, 'subscribe': None}
signal = {'port': 8185, 'subscribe': lambda x: {'event': 'subscribe', 'channel': 'signals', 'channelId': x}}



class SocketClient:
    def __init__(self, data, param=None):
        self.url = f'ws://platotradeinfo.silencatech.com:{data["port"]}'
        self.subscribe = data['subscribe']
        self.param = param
        self.ws = websocket.WebSocketApp(self.url,
                                         on_message=self.on_message,
                                         on_error=self.on_error,
                                         on_close=self.on_close)

    def on_message(self, ws, message):
        print(message)

    def on_error(self, ws, error):
        print(error)

    def on_close(self, ws):
        print("### closed ###")

    def on_open(self):
        def run(*args):
            def connect(send):
                if self.subscribe is not None:
                    time.sleep(1)
                    self.ws.send(json.dumps(send))
                time.sleep(1)
                # ws.close()
                print("thread terminating...")
            if type(self.param) in (range, list):
                for i in self.param:
                    connect(self.subscribe(i))
            elif self.param is None and self.subscribe is not None:
                connect(self.subscribe)
            elif self.param is not None:
                connect(self.subscribe(self.param))
            else:
                pass

        thread.start_new_thread(run, ())


if __name__ == "__main__":
    websocket.enableTrace(True)
    socket = SocketClient(signal, param=[76])
    socket.ws.on_open = socket.on_open()

    socket.ws.run_forever()


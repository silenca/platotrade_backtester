import logging


def setup_loggin():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(funcName)s %(levelname)s %(message)s',
                        filename='myapp.log',
                        filemode='a')
    return logging
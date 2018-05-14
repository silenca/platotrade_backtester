import logging


def setup_loggin(filename='logger.log'):
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(funcName)s %(levelname)s %(message)s',
                        filename=filename,
                        filemode='a')
    return logging
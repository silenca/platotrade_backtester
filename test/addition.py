import os
import pandas as pd

path = os.getcwd()


def get_data_from_file():
    df = pd.read_csv(os.path.join(path, 'tmp/data.csv'))

    return df.rename(columns={"vo": 'volume',
                              "h": "high",
                              'c': 'close',
                              'o': 'open',
                              'l': 'low'})


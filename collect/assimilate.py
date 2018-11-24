from . import google, reddit, fourchan, binance

import pandas as pd
import functools
from scipy import signal, ndimage
import numpy as np


class DataAssimilator(object):
    def __init__(self, coin, start_date, end_date):
        self.coin = coin

        self.start_date = start_date
        self.end_date = end_date

        self.data_dfs = []

    def add_google_trends(self):
        collector_dfs = self.add_collector(google.GoogleTrends)
        for df in collector_dfs:
            df = df.apply(self.notch_filter, raw=True)
            # df = df.apply(self.gaussian_filter, raw=True)
            # df = df.apply(self.normalize, raw=True)

            df = self.add_collector_label(df, 'gtrends')

            self.data_dfs.append(df)

    def add_reddit_comments(self):
        collector_dfs = self.add_collector(reddit.RedditComments)
        for df in collector_dfs:
            df = df.apply(self.gaussian_filter, raw=True)
            df = df.apply(self.notch_filter, raw=True)
            # df = df.apply(self.normalize, raw=True)

            df = self.add_collector_label(df, 'reddit')

            self.data_dfs.append(df)

    def add_fourchan_comments(self):
        collector_dfs = self.add_collector(fourchan.FourChanComments)
        for df in collector_dfs:
            df = df.apply(self.gaussian_filter, raw=True)
            df = df.apply(self.notch_filter, raw=True)
            # df = df.apply(self.normalize, raw=True)

            df = self.add_collector_label(df, '4chan')

            self.data_dfs.append(df)

    def add_binance_exchange(self):
        collector_dfs = self.add_collector(binance.Binance,
                                           keywords=['symbol'])
        for df in collector_dfs:
            # df = df.apply(self.notch_filter, raw=True, quality=0.5)
            # df = df.apply(self.gaussian_filter, raw=True)
            # df = df.apply(self.normalize, raw=True)

            df = self.add_collector_label(df, 'binance')

            self.data_dfs.append(df)

    def add_keyword_label(self, dataframe, keyword):
        columns = dataframe.columns
        new_columns = {column: '{0}_"{1}"'.format(column, keyword) for
                       column in columns}
        return dataframe.rename(columns=new_columns)

    def add_collector_label(self, dataframe, collector_name):
        columns = dataframe.columns
        new_columns = {column: '{0}_{1}'.format(collector_name, column) for
                       column in columns}
        return dataframe.rename(columns=new_columns)

    def add_collector(self, collector, keywords=['name', 'symbol']):
        collector_dfs = []
        for keyword in keywords:
            keyword = getattr(self.coin, keyword)
            data_collector = collector(keyword=keyword,
                                       start_date=self.start_date,
                                       end_date=self.end_date)
            data = data_collector.compile()

            data = self.add_keyword_label(data, keyword)

            collector_dfs.append(data)

        return collector_dfs

    def get_data(self):
        index_name = 'data_start'

        assimilated_df = functools.reduce(lambda left, right:
                                          pd.merge(left, right,
                                                   on=index_name, how='outer'),
                                          self.data_dfs)

        return assimilated_df

    def notch_filter(self, data, freq=1/24.0, quality=0.05):
        b, a = signal.iirnotch(freq, quality)
        y = abs(signal.filtfilt(b, a, data))
        return y

    def normalize(self, data):
        return data / np.max(data) * 100

    def gaussian_filter(self, data, sigma=1):
        return ndimage.gaussian_filter1d(data, sigma)

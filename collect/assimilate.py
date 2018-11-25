from . import google, reddit, fourchan, binance
from . import coinmarketcap

import pandas as pd
import functools
import json


class Coin(object):

    def __init__(self, symbol):
        self.symbol = symbol
        with open('config.json') as config_file:
            config = json.load(config_file)

        data_df = coinmarketcap.CoinMarketCapMetaData(
            symbol, config['coinmarketcap_api_key']).compile()
        data = {col: data_df.iloc[0][i]
                for i, col in enumerate(data_df.columns)}

        self.name = data['name']
        self.category = data['category']
        self.logo = data['logo']
        self.website = data['website']
        self.source_code = data['source_code']
        self.message_board = data['message_board']
        self.announcement = data['announcement']
        self.reddit = data['reddit']
        self.twitter = data['twitter']

        print(self.source_code)


class DataAssimilator(object):
    def __init__(self, start_date, end_date, coin, reference_coin='BTC'):
        self.coin = Coin(coin)
        self.reference_coin = Coin(reference_coin)

        self.start_date = start_date
        self.end_date = end_date

        self.data_series = []

    def add_google_trends(self, filters=None):
        self.add_collector(google.GoogleTrends,
                           filters=filters)

    def add_reddit_comments(self, filters=None):
        self.add_collector(reddit.RedditComments,
                           filters=filters)

    def add_fourchan_comments(self, filters=None):
        self.add_collector(fourchan.FourChanComments,
                           filters=filters)

    def add_binance_exchange(self, filters=None):
        self.add_collector(binance.Binance,
                           keywords=['symbol'],
                           filters=filters)

    def add_collector(self, collector, keywords=['name', 'symbol'],
                      filters=None):
        for keyword in keywords:

            keyword = getattr(self.coin, keyword)
            data_collector = collector(keyword=keyword,
                                       start_date=self.start_date,
                                       end_date=self.end_date)
            data = data_collector.compile()

            for column in data.columns:
                data_series = DataSeries(data.index.to_pydatetime(),
                                         data[column].values, filters,
                                         data_collector.name, keyword)

                self.data_series.append(data_series)

    def get_data(self):
        index_name = 'data_start'

        assimilated_df = functools.reduce(lambda left, right:
                                          pd.merge(left, right,
                                                   on=index_name, how='outer'),
                                          self.data_dfs)

        return assimilated_df


class DataSeries(object):
    def __init__(self, index, raw_data, filters, collector_name, keyword):
        self.index = index
        self.raw_data = raw_data
        self.filters = filters
        self.collector_name = collector_name
        self.keyword = keyword
        self.data = raw_data

        self.apply_filters()

    def apply_filters(self):
        for filter in self.filters:
            self.data = filter.process(self.data)

    def get_plot(self):
        pass

    def get_data(self):
        return self.index, self.data

    def get_pandas_series(self):
        pd.Series(self.data, self.index)

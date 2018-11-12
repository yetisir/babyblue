from . import google
from . import reddit
from . import fourchan
import pandas as pd
import functools


class DataAssimilator(object):
    def __init__(self, keyword_list, start_date, end_date):
        self.keyword_list = [kw.lower() for kw in keyword_list]

        self.start_date = start_date
        self.end_date = end_date

        self.data_dfs = []

    def add_google_trends(self):
        self.add_collector(google.GoogleTrends)

    def add_reddit_comments(self):
        self.add_collector(reddit.RedditComments)

    def add_fourchan_comments(self):
        self.add_collector(fourchan.FourChanComments)

    def add_collector(self, collector):

        for keyword in self.keyword_list:
            data_collector = collector(keyword=keyword,
                                       start_date=self.start_date,
                                       end_date=self.end_date)
            data = data_collector.compile()

            if data is not None:
                self.data_dfs.append(data)

    def get_data(self):
        index_name = 'data_start'
        assimilated_df = functools.reduce(lambda left, right:
                                          pd.merge(left, right,
                                                   on=index_name, how='outer'),
                                          self.data_dfs)

        return assimilated_df

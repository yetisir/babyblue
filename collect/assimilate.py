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

    def add_google_trends(self):
        self.add_collector(google.GoogleTrends)

    def add_reddit_comments(self):
        self.add_collector(reddit.RedditComments)

    def add_fourchan_comments(self):
        self.add_collector(fourchan.FourChanComments)

    def add_collector(self, collector):

        data_dfs = []
        for keyword in self.keyword_list:
            data_collector = collector(keyword=keyword,
                                       start_date=self.start_date,
                                       end_date=self.end_date)
            data = data_collector.compile()
            # data = data_collector.get_dataframe()

            if data is not None:
                data_dfs.append(data)

        index_name = data_dfs[0].index.name
        assimilated_df = functools.reduce(lambda left, right:
                                          pd.merge(left, right,
                                                   on=index_name, how='outer'),
                                          data_dfs)

        self.assimilated_df = assimilated_df

    def get_data(self):
        return self.assimilated_df

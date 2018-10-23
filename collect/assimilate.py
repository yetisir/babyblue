from . import google
from . import reddit
import pandas as pd


class DataAssimilator(object):
    def __init__(self, keyword_list, start_date, end_date):
        self.keyword_list = [kw.lower() for kw in keyword_list]

        self.start_date = start_date
        self.end_date = end_date
        self.assimilated_dfs = []

    def add_google_trends(self):
        self.add_collector(google.GoogleTrends)

    def add_reddit_comments(self):
        self.add_collector(reddit.RedditComments)

    def add_collector(self, collector):
        for keyword in self.keyword_list:
            data_collector = collector(keyword=keyword,
                                       start_date=self.start_date,
                                       end_date=self.end_date)
            data_collector.query_data()
            data = data_collector.get_dataframe()

            if any(data):
                self.assimilated_dfs.append(data)

    def get_data(self):
        return #pd.concat(self.assimilated_dfs, axis='columns')

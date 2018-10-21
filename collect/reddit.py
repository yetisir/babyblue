from . import DataCollector
from psaw import PushshiftAPI
import datetime
import pandas as pd

from sqlalchemy import Table, MetaData, Column
from sqlalchemy import Integer, DateTime, Boolean, String, Text


class RedditComments(DataCollector):
    def __init__(self, keyword, start_date, end_date,
                 sample_interval='2d', resample_interval='1h',
                 subreddit=''):

        # call the init functions of the parent class
        super().__init__(collector_name='reddit-comments',
                         keyword=keyword,
                         start_date=start_date,
                         end_date=end_date,
                         sample_interval=sample_interval)

        # defining class attributes
        self.reddit = PushshiftAPI()
        self.subreddit = subreddit
        self.resample_interval = resample_interval

    def define_cache_table(self):
        metadata = MetaData()
        self.cache_table = Table(self.collector_name, metadata,
                                 Column('keyword', String(32),
                                        primary_key=True),
                                 Column('id', Integer,
                                        primary_key=True),
                                 Column('author', String(32)),
                                 Column('timestamp', DateTime),
                                 Column('parent', Integer),
                                 Column('body', Text))
        metadata.create_all(self.cache_engine)

    def download_to_dataframe(self, interval_start, interval_end):

        results_gen = self.reddit.search_comments(
            q=self.keyword,
            subreddit=self.subreddit,
            # filter=['author', 'created_utc', 'body'],
            after=int(interval_start.replace(
                tzinfo=datetime.timezone.utc).timestamp()),
            before=int(interval_end.replace(
                tzinfo=datetime.timezone.utc).timestamp()))

        print(results_gen)

    def sql_to_dataframe(self, interval_start, interval_end):
        # self.cache_df = pd.Dataframe(cache_dict)
        query = self.interval_sql_query(interval_start, interval_end)
        query = query.order_by('timestamp')

        cache_df = pd.read_sql(sql=query.statement, con=self.session.bind)

        return cache_df

    def interval_sql_query(self, interval_start, interval_end):
        query = self.session.query(self.cache_table)
        query = query.filter(
            (self.cache_table.c.keyword == self.keyword),
            (self.cache_table.c.timestamp >= interval_start),
            (self.cache_table.c.timestamp <= interval_end))

        return query


    def is_cache_complete(self, interval_start, interval_end, cache_df):

        # return false if there are no rows in dataframe
        if not cache_df.shape[0]:
            return False

        first_interval = cache_df['data_start'].min()
        most_recent_interval = cache_df['data_start'].max()
        interval_length = cache_df['data_interval'].iloc[-1]

        if first_interval > interval_start:
            return False

        if datetime.utcnow() - most_recent_interval > interval_length:
            if most_recent_interval < interval_end:
                return False

            # return false if there are any partial rows over the interval
            if cache_df['partial'].sum():
                return False

        return True


    def handle_query_error(self, error):

        raise


        # comment_list = []
        # for comment in results_gen:
        #     comment_list.append(
        #         {'author': comment.author,
        #          'datetime': datetime.datetime.utcfromtimestamp(
        #              comment.created_utc),
        #          'comment': comment.body,
        #          '{0}_mentions'.format(
        #              self.keyword): comment.body.lower().count(self.keyword),
        #          '{0}_comments'.format(self.keyword): 1})
        #
        # comment_df = pd.DataFrame(comment_list)
        # comment_df = comment_df.set_index('datetime')
        # interval_df = comment_df.resample(self.resample_interval).sum()
        #
        # print(interval_df)
        # if (datetime.datetime.utcnow() - interval_df.index[-1] <
        #         pd.to_timedelta(self.resample_interval)):
        #     interval_df = interval_df[:-1]
        # print(interval_df)
        #
        # # return dataframe of interval
        # return interval_df

from . import DataCollector
from psaw import PushshiftAPI
from datetime import datetime, timezone
import pandas as pd

from sqlalchemy import Table, MetaData, Column
from sqlalchemy import Integer, DateTime, String, Text


class RedditComments(DataCollector):
    def __init__(self, keyword, start_date, end_date, sample_interval='2d',
                 resample_interval='1h', subreddit='cryptocurrency'):

        # call the init functions of the parent class
        super().__init__(collector_name='reddit-comments',
                         keyword=keyword,
                         start_date=start_date,
                         end_date=end_date,
                         sample_interval=sample_interval)

        # defining class attributes
        self.reddit = PushshiftAPI()
        self.subreddit = subreddit
        # convert times to datetime timedelta objects
        self.resample_interval = pd.to_timedelta(resample_interval)

    def define_cache_table(self):
        metadata = MetaData()
        self.cache_table = Table(self.collector_name, metadata,
                                 Column('keyword', String(32),
                                        primary_key=True),
                                 Column('id', Integer,
                                        primary_key=True),
                                 Column('subreddit', String(32)),
                                 Column('author', String(32)),
                                 Column('timestamp', DateTime),
                                 Column('text', Text))
        metadata.create_all(self.cache_engine)

    def download_to_dataframe(self, interval_start, interval_end):

        results_gen = self.reddit.search_comments(
            q=self.keyword,
            subreddit=self.subreddit,
            filter=['id', 'subreddit', 'author', 'created_utc', 'body'],
            after=int(interval_start.replace(
                tzinfo=timezone.utc).timestamp()),
            before=int(interval_end.replace(
                tzinfo=timezone.utc).timestamp()))

        cache_df = pd.DataFrame(results_gen)

        if cache_df.empty:
            return cache_df

        cache_df['keyword'] = self.keyword

        cache_df = cache_df.rename(columns={'created_utc': 'timestamp',
                                            'body': 'text'})

        cache_df['timestamp'] = pd.to_datetime(cache_df['timestamp'], unit='s')

        cache_df = cache_df[['keyword',
                             'id',
                             'subreddit',
                             'author',
                             'timestamp',
                             'text']]
        return cache_df

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

    def remove_interval_from_cache(self, interval_start, interval_end):
        pass

    def is_cache_complete(self, interval_start, interval_end, cache_df):
        if self.existing_interval == (interval_start, interval_end):
            return True
        else:
            return False

    def handle_download_error(self, error):

        raise

    def download_intervals(self):
        sql_query = self.interval_sql_query(self.start_date, self.end_date)

        max_existing_date = sql_query.order_by(
            self.cache_table.c.timestamp.desc()).first()
        min_existing_date = sql_query.order_by(
            self.cache_table.c.timestamp.asc()).first()

        if min_existing_date and max_existing_date:
            min_existing_date = min_existing_date.timestamp
            max_existing_date = max_existing_date.timestamp

        interval_bounds = []
        num_intervals = ((self.end_date - self.start_date) //
                         self.sample_interval) + 2

        for i in range(num_intervals):
            bound = self.start_date + i * self.sample_interval
            bound = min(bound, self.end_date)

            if min_existing_date and max_existing_date:
                if bound < min_existing_date or bound > max_existing_date:
                    interval_bounds.append(bound)
            else:
                    interval_bounds.append(bound)

        if max_existing_date and min_existing_date:
            interval_bounds.append(max_existing_date)
            interval_bounds.append(min_existing_date)

        interval_bounds.sort()

        start_intervals = interval_bounds[:-1]
        end_intervals = interval_bounds[1:]

        intervals = [i for i in zip(start_intervals, end_intervals)]

        self.existing_interval = (min_existing_date, max_existing_date)

        return intervals




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

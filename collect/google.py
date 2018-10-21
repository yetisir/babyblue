from . import DataCollector
from pytrends.request import TrendReq
import pandas as pd
import time
from datetime import datetime

from sqlalchemy import Table, MetaData, Column
from sqlalchemy import Integer, DateTime, Boolean, String


class GoogleTrends(DataCollector):
    def __init__(self, keyword, start_date, end_date, category=0,
                 sample_interval='2d', overlap_interval='1d', wait=1,
                 sleep=60, time_format='%Y-%m-%dT%H'):

        # call the init functions of the parent class
        super().__init__(collector_name='google-trends',
                         keyword=keyword,
                         start_date=start_date,
                         end_date=end_date,
                         sample_interval=sample_interval,
                         overlap_interval=overlap_interval,
                         wait=wait,
                         time_format=time_format)

        # defining class attributes
        self.sleep = sleep
        self.category = category
        self.pytrend = TrendReq()

        # cache_table
        self.define_cache_table()

    def define_cache_table(self):
        metadata = MetaData()
        self.cache_table = Table(self.collector_name, metadata,
                                 Column('keyword', String(16),
                                        primary_key=True),
                                 Column('query_start', DateTime,
                                        primary_key=True),
                                 Column('query_interval', DateTime,
                                        primary_key=True),
                                 Column('data_start', DateTime,
                                        primary_key=True),
                                 Column('data_interval', DateTime),
                                 Column('trend', Integer),
                                 Column('partial', Boolean))
        metadata.create_all(self.cache_engine)

    def download_to_dataframe(self, interval_start, interval_end):
        # convert the interval bounds to strings
        interval_start_str = interval_start.strftime(self.time_format)
        interval_end_str = interval_end.strftime(self.time_format)

        # send request to google for the trend data
        timeframe = '{0} {1}'.format(interval_start_str, interval_end_str)
        self.pytrend.build_payload([self.keyword], cat=self.category,
                                   timeframe=timeframe)
        interval_df = self.pytrend.interest_over_time()

        # return dataframe of interval
        cache_df = interval_df
        cache_df['keyword'] = self.keyword
        cache_df['query_start'] = interval_start
        cache_df['query_interval'] = (self.sample_interval +
                                      self.overlap_interval) + self.epoch
        cache_df['data_start'] = pd.to_datetime(interval_df.index)
        cache_df['data_interval'] = (interval_df.index[1] -
                                     interval_df.index[0]) + self.epoch
        cache_df['isPartial'] = cache_df['isPartial'].astype(bool).astype(int)

        cache_df = cache_df.rename(columns={self.keyword: 'trend',
                                            'isPartial': 'partial'})
        cache_df = cache_df.reset_index(drop=True)

        cache_df = cache_df[['keyword',
                             'query_start',
                             'query_interval',
                             'data_start',
                             'data_interval',
                             'trend',
                             'partial']]

        return cache_df

    def handle_download_error(self, error):

        # catch blocked ip error. this may happen if there are too many request
        # over a short period of time. this is sometimes resolved witha one
        # minute buffer
        if str(error)[-4:-1] == '429':
            # TODO replace with more appropriate logging method
            print('QUERY LIMIT REACHED: waiting {0} '
                  'seconds'.format(self.sleep))
            time.sleep(self.sleep)
            return self.query_interval()
        else:
            raise

    def sql_to_dataframe(self, interval_start, interval_end):
        # self.cache_df = pd.Dataframe(cache_dict)
        query = self.interval_sql_query(interval_start, interval_end)
        query = query.order_by('data_start')

        cache_df = pd.read_sql(sql=query.statement, con=self.session.bind)
        cache_df['query_interval'] = cache_df['query_interval'] - self.epoch
        cache_df['data_interval'] = cache_df['data_interval'] - self.epoch

        return cache_df

    def interval_sql_query(self, interval_start, interval_end):
        query = self.session.query(self.cache_table)
        query = query.filter(
            (self.cache_table.c.keyword == self.keyword),
            (self.cache_table.c.query_start == interval_start),
            (self.cache_table.c.query_interval ==
             ((self.sample_interval + self.overlap_interval) + self.epoch)),
            (self.cache_table.c.data_start >= interval_start),
            (self.cache_table.c.data_start <= interval_end))

        return query

    def remove_interval_from_cache(self, interval_start, interval_end):
        query = self.interval_sql_query(interval_start, interval_end)
        query.delete(synchronize_session=False)
        self.session.commit()

    def cache_interval(self, interval_start, interval_end, cache_df):

        self.remove_interval_from_cache(interval_start, interval_end)
        cache_df.to_sql(self.collector_name, self.cache_engine,
                        if_exists='append', index=False)

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


    def merge_overlap(self):
        pass
        # # get timedelta of existing data
        # df_time_range = self.keyword_df.index[-1] - self.keyword_df.index[0]
        #
        # # select all of overalp data if it exists
        # if df_time_range > self.overlap_interval:
        #     overlap_start = self.interval_start
        #     overlap_end = self.interval_start + self.overlap_interval
        #
        # # otherwise only select range of existing data to merge
        # else:
        #     overlap_start = self.keyword_df.index[0]
        #     overlap_end = self.keyword_df.index[-1]
        #
        # # get overlapping series
        # previous_interval_overlap = self.keyword_df.loc[overlap_start:,
        #                                                 self.keyword]
        # current_interval_overlap = self.interval_df.loc[:overlap_end,
        #                                                 self.keyword]
        #
        # # get averages of overlapping series
        # previous_average = previous_interval_overlap.mean()
        # current_average = current_interval_overlap.mean()
        #
        # # normalize new interval based on the overlap with the old interval
        # self.interval_df.loc[:, self.keyword] = (
        #     self.interval_df[self.keyword] * (previous_average /
        #                                       current_average))
        #
        # # chop off overlapping data from new interval
        # self.interval_df = self.interval_df.loc[(self.interval_start +
        #                                          self.overlap_interval):]
        # self.interval_df = self.interval_df.loc[self.interval_df.index[1]:]

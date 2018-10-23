from . import DataCollector
from pytrends.request import TrendReq
import pandas as pd
import time
from datetime import datetime

from sqlalchemy import Table, MetaData, Column
from sqlalchemy import Integer, DateTime, Boolean, String


class GoogleTrends(DataCollector):
    def __init__(self, keyword, start_date, end_date, category=0,
                 sample_interval='2d', overlap_interval='1d',
                 resample_interval='1h', wait=1, sleep=60):

        # call the init functions of the parent class
        super().__init__(collector_name='google-trends',
                         keyword=keyword,
                         start_date=start_date,
                         end_date=end_date,
                         sample_interval=sample_interval,
                         resample_interval=resample_interval)

        # defining class attributes
        self.wait = wait
        self.sleep = sleep
        self.category = category
        self.pytrend = TrendReq()

        self.request_time = time.time()

        # convert times to datetime timedelta objects
        self.overlap_interval = pd.to_timedelta(overlap_interval)

        # defining query limits based on the epoch and interval so that they
        # are consistent regardless of the specified start date
        self.epoch = datetime.utcfromtimestamp(0)

    def define_cache_table(self):
        metadata = MetaData()
        self.cache_table = Table(self.collector_name, metadata,
                                 Column('keyword', String(32),
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
        time_format = '%Y-%m-%dT%H'

        # convert the interval bounds to strings
        interval_start_str = interval_start.strftime(time_format)
        interval_end_str = interval_end.strftime(time_format)

        # send request to google for the trend data
        timeframe = '{0} {1}'.format(interval_start_str, interval_end_str)
        self.pytrend.build_payload([self.keyword], cat=self.category,
                                   timeframe=timeframe)

        # allow wait time between requests
        time.sleep(max(0, self.wait - (time.time() - self.request_time)))
        interval_df = self.pytrend.interest_over_time()
        self.request_time = time.time()
        if interval_df.empty:
            num_subintervals = ((interval_end - interval_start) //
                                self.resample_interval) + 1
            empty_result = []
            for i in range(num_subintervals):
                start_time = interval_start + i * self.resample_interval
                end_time = start_time + self.resample_interval
                partial = True if end_time > datetime.utcnow() else False
                empty_result.append([start_time, 0, partial])

            interval_df = pd.DataFrame(empty_result, columns=['date',
                                                              self.keyword,
                                                              'isPartial'])
            interval_df = interval_df.set_index('date')

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

    def handle_download_error(self, interval_start, interval_end, error):

        # catch blocked ip error. this may happen if there are too many request
        # over a short period of time. this is sometimes resolved witha one
        # minute buffer
        if str(error)[-4:-1] == '429':
            # TODO replace with more appropriate logging method
            self.status('QUERY LIMIT REACHED', interval_start, interval_end)
            time.sleep(self.sleep)
            return self.query_interval(interval_start, interval_end)
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

    def post_cache_routine(self, interval_start, interval_end):
        pass

    def pre_cache_routine(self, interval_start, interval_end):
        # remove partial interval from cache
        query = self.interval_sql_query(interval_start, interval_end)
        query.delete(synchronize_session=False)
        self.session.commit()

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

    def download_intervals(self):

        # TODO: Generate intervals more eloquently

        # push back the start date so that there is an natural number of
        # intervals between the epoch and the start
        query_start = (self.start_date - (self.start_date - self.epoch) %
                       self.sample_interval)

        # push forward the end date so that there are a natural number of
        # intervals between the start and end dates
        query_end = (self.end_date - (self.end_date - query_start) %
                     self.sample_interval + self.sample_interval)

        # determine the total number of intervals to be queried
        num_intervals = ((query_end - query_start) // self.sample_interval)

        intervals = []

        # loop through each interval and compile a dataset
        for i in range(num_intervals):

            # determing the bounding datetimes for the interval
            interval_start = (query_start + i * self.sample_interval -
                              self.overlap_interval)
            interval_end = (query_start + (i + 1) * self.sample_interval)

            intervals.append((interval_start, interval_end))

        return intervals

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

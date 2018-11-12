from . import DataCollector
from pytrends.request import TrendReq
import pandas as pd
import time
from datetime import datetime
from sqlalchemy import Column, Integer, DateTime, Boolean, String


class GoogleTrends(DataCollector):
    def __init__(self, keyword, start_date, end_date, category=0,
                 sample_interval='5d', overlap_interval='1d',
                 data_resolution='1h', wait=1, sleep=00):

        # unique coverage_identifier for reproduceable data
        self.coverage_interval = (pd.to_timedelta(sample_interval) +
                                  datetime.utcfromtimestamp(0))

        # call the init functions of the parent class
        super().__init__(collector_name='google-trends',
                         keyword=keyword,
                         start_date=start_date,
                         end_date=end_date,
                         sample_interval=sample_interval,
                         data_resolution=data_resolution)

        # defining class attributes
        self.wait = wait
        self.sleep = sleep
        self.category = category
        self.pytrend = TrendReq(hl='')

        self.request_time = time.time()

        # convert times to datetime timedelta objects
        self.overlap_interval = pd.to_timedelta(overlap_interval)

    def define_cache_table(self, Base):

        cache = {'__tablename__': self.collector_name,
                 'keyword': Column('keyword', String(32),
                                   primary_key=True),
                 'query_start': Column(DateTime,
                                       primary_key=True),
                 'query_interval': Column(DateTime,
                                          primary_key=True),
                 'data_start': Column(DateTime,
                                      primary_key=True),
                 'data_interval': Column(DateTime),
                 'trend': Column(Integer),
                 'partial': Column(Boolean)}

        self.cache_table = type('Cache', (Base, ), cache)

    def download_to_dataframe(self, interval_start, interval_end):
        # define time format required for google trends api
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

        # handle the empty dataframe case. if pytrends returns an empty
        # dataframe, populate one with 0 trend
        if interval_df.empty:

            # calculate number of intervals
            num_subintervals = ((interval_end - interval_start) //
                                self.data_resolution) + 1

            # create intervals in a list
            empty_result = []
            for i in range(num_subintervals):
                start_time = interval_start + i * self.data_resolution
                end_time = start_time + self.data_resolution
                partial = True if end_time > datetime.utcnow() else False
                empty_result.append([start_time, 0, partial])

            # create dataframe to complete interval but with 0 trend data
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

        # return downloaded data as dataframe
        return cache_df

    def handle_download_error(self, interval_start, interval_end, error):

        # catch blocked ip error. this may happen if there are too many request
        # over a short period of time. this is sometimes resolved witha one
        # minute buffer
        error_code = str(error)[-4:-1]
        if error_code == '429':
            # update status source and display status
            self.status('QUERY LIMIT REACHED', interval_start, interval_end)
            if self.sleep:
                time.sleep(self.sleep)
                return self.query_interval(interval_start, interval_end)
            else:
                pass
        elif error_code == '500':
            self.status('GOOGLE SERVER ERROR', interval_start, interval_end)
            pass
        else:
            raise

    def dataframe_to_sql(self, cache_df):
        cache_df.to_sql(self.collector_name, self.cache_engine,
                        if_exists='append', index=False)

    def sql_to_dataframe(self, interval_start, interval_end):
        # load data from cache
        query = self.interval_sql_query(interval_start, interval_end)
        query = query.order_by('data_start')
        cache_df = pd.read_sql(sql=query.statement, con=self.session.bind)

        # convert back to timedeltas
        cache_df['query_interval'] = cache_df['query_interval'] - self.epoch
        cache_df['data_interval'] = cache_df['data_interval'] - self.epoch

        # return cached data as dataframe
        return cache_df

    def interval_sql_query(self, interval_start, interval_end):
        # generate the sql query for retrieving cached data
        query = self.session.query(self.cache_table)
        query = query.filter(
            (self.cache_table.keyword == self.keyword),
            (self.cache_table.query_start >= interval_start),
            (self.cache_table.query_start < interval_end),
            (self.cache_table.query_interval ==
             ((self.sample_interval + self.overlap_interval) + self.epoch)),
            (self.cache_table.data_start >= interval_start),
            (self.cache_table.data_start <= interval_end))

        # return the cache query
        return query

    def post_cache_routine(self, interval_start, interval_end):
        # no post-cache routine required
        pass

    def pre_cache_routine(self, interval_start, interval_end):
        query = self.interval_sql_query(interval_start, interval_end)
        query.delete(synchronize_session=False)
        self.session.commit()

    def download_intervals(self):
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

            # add new_interval to list
            intervals.append((interval_start, interval_end))

        # return list of intervals
        return intervals

    def process_raw_data(self, data_df):
        data_df = self.merge_overlap(data_df)

        return data_df.loc[self.start_date:self.end_date]

    def merge_overlap(self, data_df):

        intervals = self.download_intervals()

        keyword_df = pd.DataFrame()
        for interval_start, interval_end in intervals:
            interval_df = data_df[(data_df['query_start'] ==
                                   interval_start) &
                                  (data_df['data_start'] >=
                                   interval_start) &
                                  (data_df['data_start'] <
                                   interval_end)].copy()

            interval_df = interval_df.set_index('data_start')
            if keyword_df.empty:
                keyword_df = interval_df

            else:
                previous_times = keyword_df.index
                interval_times = interval_df.index

                overlap = previous_times.intersection(interval_times)

                top_overlap_df = keyword_df.loc[overlap]
                bottom_overlap_df = interval_df.loc[overlap]

                normalization_factor = (top_overlap_df['trend'].mean() /
                                        bottom_overlap_df['trend'].mean())
                interval_df.loc[:, 'trend'] *= normalization_factor

                interval_df = interval_df.loc[interval_start +
                                              self.overlap_interval:]

                keyword_df = keyword_df.append(interval_df)

        keyword_df = keyword_df[['trend']]
        return keyword_df

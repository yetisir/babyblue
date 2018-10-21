from datetime import datetime
import os
import time
import pandas as pd
import sqlalchemy
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker


class DataCollector(object):
    def __init__(self, collector_name, keyword, start_date, end_date,
                 sample_interval, overlap_interval=0, wait=1,
                 time_format='%Y-%m-%dT%H', cache_name='cache.sqlite'):

        self.collector_name = collector_name
        self.keyword = keyword
        self.cache_name = cache_name
        self.time_format = time_format
        self.wait = wait
        self.start_date = datetime.combine(start_date, datetime.min.time())
        self.end_date = datetime.combine(end_date, datetime.min.time())

        # convert times to datetime timedelta objects
        self.sample_interval = pd.to_timedelta(sample_interval)
        self.overlap_interval = pd.to_timedelta(overlap_interval)

        # defining query limits based on the epoch and interval so that they
        # are consistent regardless of the specified start date
        self.epoch = datetime.utcfromtimestamp(0)

        # push back the start date so that there is an natural number of
        # intervals between the epoch and the start
        self.query_start = (self.start_date - (self.start_date - self.epoch) %
                            self.sample_interval)

        # push forward the end date so that there are a natural number of
        # intervals between the start and end dates
        self.query_end = (self.end_date - (self.end_date - self.query_start) %
                          self.sample_interval + self.sample_interval)

        # determine the total number of intervals to be queried
        self.num_intervals = ((self.query_end - self.query_start) //
                              self.sample_interval)

        # initialize empty dataframe to store all collected data
        self.keyword_df = pd.DataFrame()

        # initialize sqlite database for caching downloaded data
        self.cache_engine = self.create_cache_engine()

        Session = sessionmaker(bind=self.cache_engine)
        self.session = Session()

        self.display_source = ''

    def query_data(self):
        # loop through each interval and compile a dataset
        for i in range(self.num_intervals):
            # determing the bounding datetimes for the interval
            interval_start = (self.query_start + i * self.sample_interval -
                              self.overlap_interval)
            interval_end = (self.query_start + (i + 1) * self.sample_interval)

            # query interval
            cache_df = self.query_interval(interval_start, interval_end)

            # merge data if specified and not the first interval
            if self.overlap_interval and len(self.keyword_df):
                self.merge_overlap()
            # concatenate the interval to the dataframe
            if len(self.keyword_df):
                self.keyword_df = self.keyword_df.append(cache_df,
                                                         sort=False)
            else:
                self.keyword_df = cache_df

        # chop dataframe to originally requested size
#        self.keyword_df = self.keyword_df.loc[self.start_date:self.end_date]
        self.next_status()

    def status(self, message, interval_start, interval_end):
        fmt_str = ('{message: <20} {collector} - {keyword: <15} | '
                   '{start} to {end}')

        if self.display_source != self.source:
            self.next_status()

        print(fmt_str.format(message='{message}:'.format(message=message),
                             collector=self.collector_name,
                             keyword=self.keyword,
                             start=interval_start.date(),
                             end=interval_end.date()), end='\r')

        self.display_source = self.source

    def next_status(self):
        print()

    def query_interval(self, interval_start, interval_end):

        # download the interval again if it does not exist or is incomplete
        cache_df = self.sql_to_dataframe(interval_start, interval_end)

        if self.is_cache_complete(interval_start, interval_end, cache_df):
            self.source = 'cache'
            self.status('LOADING FROM CACHE', interval_start, interval_end)
            return cache_df
        try:
            # TODO replace with more appropriate logging method
            self.source = 'download'
            self.status('DOWNLOADING', interval_start, interval_end)

            # query method is defined by the child class
            cache_df = self.download_to_dataframe(interval_start, interval_end)
            self.cache_interval(interval_start, interval_end, cache_df)
            time.sleep(self.wait)

            return cache_df

        # catch the exception and pass to child class
        except Exception as error:
            # handle_query_error method is defined by the child class
            self.source = 'failed'
            self.status('DOWNLOAD FAILED', interval_start, interval_end)
            cache_df = self.handle_download_error(error)

            return cache_df

    def create_cache_engine(self):
        # full path to the cache database
        cache_path = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                                  os.path.pardir,
                                                  'cache',
                                                  self.cache_name))

        # create sqlite database natural
        cache = {'drivername': 'sqlite',
                 'database': cache_path}

        # return databse engine
        return sqlalchemy.create_engine(URL(**cache), echo=False)

    def get_dataframe(self):

        # return the dataframe with the collected data
        return self.keyword_df

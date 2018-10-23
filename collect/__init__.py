from datetime import datetime
import os
import pandas as pd
import sqlalchemy
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker


class DataCollector(object):
    def __init__(self, collector_name, keyword, start_date, end_date,
                 sample_interval, resample_interval,
                 cache_name='cache.sqlite'):

        # setting class attributes
        self.collector_name = collector_name
        self.cache_name = cache_name

        # converst keyword to lowercase to avoid duplicate searches
        self.keyword = keyword.lower()

        # convert dates to datetime objects if they arent already
        self.start_date = start_date
        self.end_date = end_date
        if self.end_date > datetime.utcnow():
            self.end_date = datetime.utcnow()

        # convert times to datetime timedelta objects
        self.sample_interval = pd.to_timedelta(sample_interval)
        self.resample_interval = pd.to_timedelta(resample_interval)

        # initialize empty dataframe to store all collected data
        self.keyword_df = pd.DataFrame()

        # initialize sqlite database for caching downloaded data
        self.cache_engine = self.create_cache_engine()

        # create cache database session to access sqlite caceh
        Session = sessionmaker(bind=self.cache_engine)
        self.session = Session()

        # create the sqlite cache_table
        self.define_cache_table()

        # variable for dispalying status
        self.display_source = ''

    def query_data(self):

        # obtain set of intervals to query
        intervals = self.download_intervals()

        # loop through each interval and compile a dataset
        for interval_start, interval_end in intervals:

            # query the specified interval
            cache_df = self.query_interval(interval_start, interval_end)

            # TO DO: all this - merge data
            # merge data if specified and not the first interval
            # if self.overlap_interval and len(self.keyword_df):
            #     self.merge_overlap()

            # concatenate the interval to the dataframe
            # if len(self.keyword_df):
            #     self.keyword_df = self.keyword_df.append(cache_df,
            #                                              sort=False)
            # else:
            #     self.keyword_df = cache_df

            self.keyword_df = cache_df
        # chop dataframe to originally requested size
        # self.keyword_df = self.keyword_df.loc[self.start_date:self.end_date]
        self.next_status()

    def status(self, message, interval_start, interval_end):
        # define display format for messages
        fmt_str = ('{message: <20} {collector} - {keyword: <15} | '
                   '{start} to {end}')

        # add a new line if the source (ie cache or download) changes
        if self.display_source != self.source:
            self.next_status()

        # display status to console with \r so that the line gets overwritten
        print(fmt_str.format(message='{message}:'.format(message=message),
                             collector=self.collector_name,
                             keyword=self.keyword,
                             start=interval_start.date(),
                             end=interval_end.date()), end='\r')

        # record the current source of this message
        self.display_source = self.source

    def next_status(self):
        # adds a new line for updating the next status message
        print()

    def query_interval(self, interval_start, interval_end):
        # download the interval again if it does not exist or is incomplete
        # sql to dataframe is defined by child class
        cache_df = self.sql_to_dataframe(interval_start, interval_end)

        # check to see if cache is complete based on the specified interval
        # if cache is complete, load it from db, otherwise download
        # is_chache_complete is defined by child class
        if self.is_cache_complete(interval_start, interval_end, cache_df):

            # updating source for status update
            self.source = 'cache'
            self.status('LOADING FROM CACHE', interval_start, interval_end)

            # return the downloaded dataframe
            return cache_df

        # if the cache does not exists or is not complete, attempt to download
        # the data from the server.
        try:

            # update status source and display status
            self.source = 'download'
            self.status('DOWNLOADING', interval_start, interval_end)

            # download the data for the interval specified and return as df
            # download_to_dataframe is defined by child class
            cache_df = self.download_to_dataframe(interval_start, interval_end)

            # save the downloaded data to the sqlite cache
            self.cache_interval(interval_start, interval_end, cache_df)

            # return the downloaded data as a dataframe
            return cache_df

        # catch the exception and pass to child class
        except Exception as error:

            # update status source and display status
            self.source = 'failed'
            self.status('DOWNLOAD FAILED', interval_start, interval_end)

            # call function to handle the error
            # handle_download_error method is defined by the child class
            cache_df = self.handle_download_error(interval_start,
                                                  interval_end, error)

            # return downloaded data as a dataframe
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

    def cache_interval(self, interval_start, interval_end, cache_df):
        # run any pre-cache routiens defined in the child class
        self.pre_cache_routine(interval_start, interval_end)

        # cache the downloaded data if there is data to cache
        if not cache_df.empty:
            cache_df.to_sql(self.collector_name, self.cache_engine,
                            if_exists='append', index=False)

        # run any post-cache routiens defined in the child class
        self.post_cache_routine(interval_start, interval_end)

    def get_dataframe(self):

        # return the dataframe with the collected data
        return self.keyword_df

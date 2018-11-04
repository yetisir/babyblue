import os
import pandas as pd
import sqlalchemy
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker

from datetime import datetime, timedelta

from sqlalchemy import Table, MetaData, Column
from sqlalchemy import Integer, DateTime, String, Text


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

        # initialize a metadata instance for the sqlite cache
        self.metadata = MetaData()

        # create the sqlite cache_table
        self.define_cache_table()
        self.define_cache_coverage_table()

        # add tables to the database
        self.metadata.create_all(self.cache_engine)

        # load existing coverage
        self.load_coverage()

        # variable for dispalying status
        self.display_source = ''

    def query_data(self):

        # obtain set of intervals to query
        intervals = self.download_intervals()

        # loop through each interval and compile a dataset
        for interval_start, interval_end in intervals:

            # query the specified interval
            cache_df = self.query_interval(interval_start, interval_end)

            # TO DO: all this - merge data - maybe move to separate class
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
        # remove interval from cache if it exists
        self.pre_cache_routine(interval_start, interval_end)

        # run any pre-cache routiens defined in the child class
        self.remove_interval_from_cache(interval_start, interval_end)

        # cache the downloaded data if there is data to cache
        if not cache_df.empty:
            cache_df.to_sql(self.collector_name, self.cache_engine,
                            if_exists='append', index=False)

        self.update_coverage(interval_start, interval_end)

        # run any post-cache routiens defined in the child class
        self.post_cache_routine(interval_start, interval_end)

    def get_dataframe(self):

        # return the dataframe with the collected data
        return self.keyword_df

    def is_cache_complete(self, interval_start, interval_end, cache_df):
        # test if the interval is contained within the bounds of any of the
        # coverage intervals
        max_coverage_end = datetime.utcfromtimestamp(0)
        for coverage_start, coverage_end in self.coverage:
            max_coverage_end = max(max_coverage_end, coverage_end)
            if (interval_start >= coverage_start and
                    interval_end <= coverage_end):
                return True

        if datetime.utcnow() - max_coverage_end < self.resample_interval:
            return True

        return False

    def load_coverage(self):
        # load the coverage intervals from the sqlite database
        query = self.session.query(self.coverage_table)
        query = query.filter((self.coverage_table.c.keyword == self.keyword))
        coverage_df = pd.read_sql(sql=query.statement,
                                  con=self.session.bind)

        # create an empty dataframe if there is no coverage
        if coverage_df.empty:
            coverage_df = pd.DataFrame(data=None,
                                       columns=coverage_df.columns,
                                       index=coverage_df.index)

        # convert dataframe to nested list for easier handling
        coverage = coverage_df[['query_start', 'query_end']].values.tolist()
        for i, interval in enumerate(coverage):
            for b, bound in enumerate(interval):
                coverage[i][b] = datetime.utcfromtimestamp(bound * 1e-9)

        # assign coverage as a class attribute
        self.coverage = coverage

    def define_cache_coverage_table(self):

        # create a table to store the coverage intervals
        self.coverage_table_name = '{0}-coverage'.format(self.collector_name)
        self.coverage_table = Table(self.coverage_table_name,
                                    self.metadata,
                                    Column('keyword', String(32),
                                           primary_key=True),
                                    Column('query_start', DateTime,
                                           primary_key=True),
                                    Column('query_end', DateTime))

    def remove_interval_from_cache(self, interval_start, interval_end):
        # remove interval from cache if there is duplicate data
        query = self.interval_sql_query(interval_start, interval_end)
        query.delete(synchronize_session=False)
        self.session.commit()

    def update_coverage_list(self, interval_start, interval_end):
        # after the cache is updated, we need to update the coverage intervals
        merged_coverage = []
        coverage = self.coverage
        coverage.append([interval_start, interval_end])
        coverage = sorted(coverage, key=lambda x: x[0])

        # loop through the coverage intervals and merge any overlapping ones
        for higher in coverage:
            # add the first interval to the merged list
            if not merged_coverage:
                merged_coverage.append(higher)
            else:
                lower = merged_coverage[-1]
                # test for intersection between lower and higher:
                # we know via sorting that lower[0] <= higher[0]
                if higher[0] <= lower[1]:
                    upper_bound = max(lower[1], higher[1])
                    # replace by merged interval
                    merged_coverage[-1] = (lower[0], upper_bound)
                else:
                    # add interval to coverage list
                    merged_coverage.append(higher)
        self.coverage = [list(x) for x in merged_coverage]

    def update_coverage(self, interval_start, interval_end):
        # merge coverage list with current interval

        self.update_coverage_list(interval_start,
                                  min(datetime.utcnow(), interval_end))

        # convert coverge list to dataframe
        coverage_df = pd.DataFrame(self.coverage, columns=['query_start',
                                                           'query_end'])
        coverage_df.insert(0, 'keyword', self.keyword)

        # remove coverage intervals from sqlite database
        query = self.session.query(self.coverage_table)
        query = query.filter((self.coverage_table.c.keyword == self.keyword))
        query.delete(synchronize_session=False)
        self.session.commit()

        # add new coverage intervals to sqlite database
        coverage_df.to_sql(self.coverage_table_name, self.cache_engine,
                           if_exists='append', index=False)


class CommentCollector(DataCollector):
    def __init__(self, keyword, start_date, end_date, collector_name,
                 sample_interval='31d', resample_interval='1h',
                 community_title='community'):

        self.community_title = community_title

        # call the init functions of the parent class
        super().__init__(collector_name=collector_name,
                         keyword=keyword,
                         start_date=start_date,
                         end_date=end_date,
                         sample_interval=sample_interval,
                         resample_interval=resample_interval)

    def define_cache_table(self):

        # create a table to store the data that is downloaded
        self.cache_table = Table(self.collector_name,
                                 self.metadata,
                                 Column('keyword', String(32),
                                        primary_key=True),
                                 Column('id', Integer,
                                        primary_key=True),
                                 Column(self.community_title, String(32)),
                                 Column('author', String(32)),
                                 Column('timestamp', DateTime),
                                 Column('text', Text))

    def sql_to_dataframe(self, interval_start, interval_end):
        # load data from cache
        query = self.interval_sql_query(interval_start, interval_end)
        query = query.order_by('timestamp')
        cache_df = pd.read_sql(sql=query.statement, con=self.session.bind)

        # return cached data as dataframe
        return cache_df

    def interval_sql_query(self, interval_start, interval_end):
        # generate the sql query for retrieving cached data
        query = self.session.query(self.cache_table)
        query = query.filter(
            (self.cache_table.c.keyword == self.keyword),
            (self.cache_table.c.timestamp >= interval_start),
            (self.cache_table.c.timestamp <= interval_end))

        # return the cache query
        return query

    def pre_cache_routine(self, interval_start, interval_end):
        pass

    def post_cache_routine(self, interval_start, interval_end):
        pass

    def download_intervals(self):
        # determine the number of intervals based on the interval size
        num_intervals = ((self.end_date - self.start_date) //
                         self.sample_interval) + 1

        # loop through the intervals and adjust the bounds based on
        # pre-existing coverage
        intervals = []
        for i in range(num_intervals):

            # initial bounds
            lower_bound = self.start_date + i * self.sample_interval
            upper_bound = lower_bound + self.sample_interval

            # truncate intervals at current time - this may result in slightly
            # outdated data if it is a long query. potentially move this to the
            # query loop
            if upper_bound > datetime.utcnow():
                upper_bound = datetime.utcnow()

            # define interval
            interval = [lower_bound, upper_bound]

            # compare interval against each coverage interval
            for j, coverage in enumerate(self.coverage):

                # test amount of overalp between the coverage and the interval
                overlap = max(timedelta(0), min(interval[1], coverage[1]) -
                              max(interval[0], coverage[0]))

                # adjust the interval if it straddles a coverage boundary. Note
                # that in the case where the coverage is entirely within the
                # interval, the whole interval will be flagged to eb downloaded
                # again.
                if overlap > timedelta(0):
                    if (interval[0] < coverage[1] and
                            interval[1] > coverage[1]):
                        interval[0] = coverage[1]
                    if (interval[1] > coverage[0] and
                            interval[0] < coverage[0]):
                        interval[1] = coverage[0]

            # add new_interval to list
            intervals.append(interval)

        # return list of intervals
        return intervals

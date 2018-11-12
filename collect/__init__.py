import os
import pandas as pd
from datetime import datetime, timedelta

import sqlalchemy
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy import Column, ForeignKey, DateTime, String, Text
from sqlalchemy.ext.declarative import declarative_base


class DataCollector(object):
    def __init__(self, collector_name, keyword, start_date, end_date,
                 sample_interval, data_resolution,
                 cache_name='cache.sqlite'):

        # setting class attributes
        self.collector_name = collector_name
        self.cache_name = cache_name

        # converst keyword to lowercase to avoid duplicate searches
        self.keyword = keyword.lower()

        # convert dates to datetime objects if they arent already
        self.start_date = start_date
        self.end_date = end_date
        # if self.end_date > datetime.utcnow():
        #     self.end_date = datetime.utcnow()

        # convert times to datetime timedelta objects
        self.sample_interval = pd.to_timedelta(sample_interval)
        self.data_resolution = pd.to_timedelta(data_resolution)

        # initialize sqlite database for caching downloaded data
        self.cache_engine = self.create_cache_engine()

        # create cache database session to access sqlite caceh
        Session = sessionmaker(bind=self.cache_engine)
        self.session = Session()

        # initialize a metadata instance for the sqlite cache
        Base = declarative_base()

        # create the sqlite cache_table
        self.define_cache_table(Base)
        self.define_cache_coverage_table(Base)

        # add tables to the database
        Base.metadata.create_all(self.cache_engine)

        # defining query limits based on the epoch and interval so that they
        # are consistent regardless of the specified start date
        self.epoch = datetime.utcfromtimestamp(0)

        # get the intervals that have already been queried previously and are
        # in the sqlite cache
        self.load_coverage()

        # variable for dispalying status
        self.display_source = ''

        self.status_start = None

    def query_data(self):

        # obtain set of intervals to query
        intervals = self.download_intervals()

        # loop through each interval and compile a dataset
        for interval_start, interval_end in intervals:

            # query the specified interval
            self.query_interval(interval_start, interval_end)

        # load full dataset from sqlite database
        self.status('LOADING FROM CACHE', self.start_date, self.end_date)
        keyword_df = self.sql_to_dataframe(intervals[0][0], intervals[-1][-1])

        self.next_status()

        return keyword_df

    def status(self, message, interval_start, interval_end):
        self.source = message

        time_since_epoch = datetime.utcnow() - self.epoch
        num_intervals = time_since_epoch // self.data_resolution
        interval_end = (num_intervals *
                        self.data_resolution) + self.epoch

        if self.status_start and self.display_source == self.source:
            interval_start = self.status_start

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
                             start=interval_start,
                             end=interval_end), end='\r')

        # record the current source of this message
        self.display_source = self.source

        self.status_start = interval_start

    def next_status(self):
        # adds a new line for updating the next status message
        print()
        self.status_start = None

    def query_interval(self, interval_start, interval_end):

        # check to see if cache is complete based on the specified interval
        # if cache is complete, load it from db, otherwise download
        # is_chache_complete is defined by child class
        if self.is_cache_complete(interval_start, interval_end):

            # updating source for status update
            self.status('FOUND IN CACHE', interval_start, interval_end)
            return
        # if the cache does not exists or is not complete, attempt to download
        # the data from the server.
        try:

            # update status source and display status
            self.status('DOWNLOADING', interval_start, interval_end)

            # download the data for the interval specified and return as df
            # download_to_dataframe is defined by child class
            cache_df = self.download_to_dataframe(interval_start, interval_end)

            # save the downloaded data to the sqlite cache
            self.cache_interval(interval_start, interval_end, cache_df)

        # catch the exception and pass to child class
        except Exception as error:

            # update status source and display status
            self.status('DOWNLOAD FAILED', interval_start, interval_end)

            # call function to handle the error
            # handle_download_error method is defined by the child class
            cache_df = self.handle_download_error(interval_start,
                                                  interval_end, error)

        # save the downloaded data to the sqlite cache
        if cache_df is not None:
            self.cache_interval(interval_start, interval_end, cache_df)

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
            self.dataframe_to_sql(cache_df)

        self.update_coverage(interval_start, interval_end)

        # run any post-cache routiens defined in the child class
        self.post_cache_routine(interval_start, interval_end)

    def is_cache_complete(self, interval_start, interval_end):
        # test if the interval is contained within the bounds of any of the
        # coverage intervals
        max_coverage_end = datetime.utcfromtimestamp(0)
        for coverage_start, coverage_end in self.coverage:
            max_coverage_end = max(max_coverage_end, coverage_end)
            if (interval_start >= coverage_start and
                    interval_end <= coverage_end):
                return True

        if interval_end > max_coverage_end:
            if datetime.utcnow() - max_coverage_end < self.data_resolution:
                return True

        return False

    def load_coverage(self):
        # load the coverage intervals from the sqlite database
        query = self.session.query(self.coverage_table)
        query = query.filter((self.coverage_table.keyword == self.keyword),
                             (self.coverage_table.coverage_interval
                              == self.coverage_interval))
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

    def define_cache_coverage_table(self, Base):
        # create a table to store the coverage intervals
        self.coverage_table_name = '{0}-coverage'.format(self.collector_name)

        coverage = {'__tablename__': self.coverage_table_name,
                    'keyword': Column('keyword', String(32), primary_key=True),
                    'query_start': Column(DateTime, primary_key=True),
                    'coverage_interval': Column(DateTime, primary_key=True),
                    'query_end': Column(DateTime)}

        self.coverage_table = type('Coverage', (Base, ), coverage)

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
        time_since_epoch = datetime.utcnow() - self.epoch
        num_intervals = time_since_epoch // self.data_resolution

        bound = (num_intervals *
                 self.data_resolution) + self.epoch

        self.update_coverage_list(interval_start,
                                  min(bound, interval_end))

        # convert coverge list to dataframe
        coverage_df = pd.DataFrame(self.coverage, columns=['query_start',
                                                           'query_end'])
        coverage_df.insert(0, 'keyword', self.keyword)
        coverage_df.insert(2, 'coverage_interval', self.coverage_interval)

        # remove coverage intervals from sqlite database
        query = self.session.query(self.coverage_table)
        query = query.filter((self.coverage_table.keyword == self.keyword))
        query.delete(synchronize_session=False)
        self.session.commit()

        # add new coverage intervals to sqlite database
        coverage_df.to_sql(self.coverage_table_name, self.cache_engine,
                           if_exists='append', index=False)

    def compile(self, download=True):
        if download:
            data_df = self.query_data()
        else:
            data_df = self.sql_to_dataframe(self.start_date, self.end_date)

        processed_df = self.process_raw_data(data_df)
        return processed_df[self.start_date:self.end_date]

class CommentCollector(DataCollector):
    def __init__(self, keyword, start_date, end_date, collector_name,
                 sample_interval='31d', data_resolution='1h',
                 community_title='community'):

        self.community_title = community_title

        self.coverage_interval = datetime.utcfromtimestamp(0)

        # call the init functions of the parent class
        super().__init__(collector_name=collector_name,
                         keyword=keyword,
                         start_date=start_date,
                         end_date=end_date,
                         sample_interval=sample_interval,
                         data_resolution=data_resolution)

    def define_cache_table(self, Base):

        # create a table to store the data that is downloaded
        self.comment_table_name = '{0}-comments'.format(self.collector_name)

        comments = {'__tablename__': self.comment_table_name,
                    'id': Column(String(32), primary_key=True),
                    self.community_title: Column(String(32)),
                    'author': Column(String(32)),
                    'timestamp': Column(DateTime),
                    'text': Column(Text)}

        self.comment_table = type('Comments', (Base, ), comments)

        cache = {'__tablename__': self.collector_name,
                 'keyword': Column('keyword', String(32),
                                   primary_key=True),
                 'comment_id': Column('comment_id', String(32),
                                      ForeignKey(self.comment_table.id),
                                      primary_key=True),
                 'comment': relationship('Comments')}

        self.cache_table = type('Cache', (Base, ), cache)

    def merge_dataframe_into_table(self, dataframe, table_name, p_keys):
        temp_table_name = 'temp'

        dataframe.to_sql(temp_table_name, self.cache_engine,
                         if_exists='replace', index=False)

        insert_string = dataframe.columns[0]
        select_string = 't.{0}'.format(dataframe.columns[0])

        for column_name in dataframe.columns[1:]:
            insert_string = '{0}, {1}'.format(insert_string, column_name)
            select_string = '{0}, t.{1}'.format(select_string, column_name)

        where_string = 't.{0} = f.{0}'.format(p_keys[0])
        for key_name in p_keys[1:]:
            where_string = '{0} AND t.{1} = f.{1}'.format(where_string,
                                                          key_name)
        with self.cache_engine.begin() as cn:
            sql = """INSERT INTO "{final_table}" ({insert_string})
                     SELECT {select_string}
                     FROM "{temp_table}" t
                     WHERE NOT EXISTS
                        (SELECT 1 FROM "{final_table}" f
                        WHERE {where_string})""".format(
                            final_table=table_name,
                            temp_table=temp_table_name,
                            insert_string=insert_string,
                            select_string=select_string,
                            where_string=where_string)
            cn.execute(sql)
            cn.execute('DROP TABLE IF EXISTS {temp}'.format(
                temp=temp_table_name))

    def dataframe_to_sql(self, cache_df):
        cache = cache_df[['keyword', 'id']]
        cache = cache.rename(columns={'id': 'comment_id'})

        comments = cache_df[['id', self.community_title, 'author', 'timestamp',
                             'text']]

        self.merge_dataframe_into_table(cache, self.collector_name,
                                        ['comment_id', 'keyword'])
        self.merge_dataframe_into_table(comments, self.comment_table_name,
                                        ['id'])

    def sql_to_dataframe(self, interval_start, interval_end):
        # load data from cache
        query = self.interval_sql_query(interval_start, interval_end)
        query = query.order_by('timestamp')
        cache_df = pd.read_sql(sql=query.statement, con=self.session.bind)

        # return cached data as dataframe
        return cache_df

    def interval_sql_query(self, interval_start, interval_end):
        # generate the sql query for retrieving cached data
        query = self.session.query(self.cache_table, self.comment_table)
        query = query.outerjoin(self.comment_table, self.comment_table.id ==
                                self.cache_table.comment_id)
        query = query.filter(
            self.cache_table.keyword == self.keyword,
            self.comment_table.timestamp >= interval_start,
            self.comment_table.timestamp <= interval_end)

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

    def process_raw_data(self, data_df):
        data_df['mentions'] = data_df['text'].apply(
            self.count_keyword_mentions)
        # data_df['sentiment'] = data_df['text'].apply(self.analyze_sentiment)

        data_df = data_df.set_index('timestamp')
        data_df = data_df.resample(self.data_resolution).sum()
        data_df.index.names = ['data_start']  # TODO: check to see if shifted
        data_df = data_df[['mentions']]

        return data_df

    def count_keyword_mentions(self, text):
        return text.lower().count(self.keyword)

    def analyze_sentiment(self, text):
        return None  # TextBlob(text).sentiment

from . import DataCollector
from psaw import PushshiftAPI
from datetime import datetime, timezone, timedelta
import pandas as pd

from sqlalchemy import Table, MetaData, Column
from sqlalchemy import Integer, DateTime, String, Text


class RedditComments(DataCollector):
    def __init__(self, keyword, start_date, end_date, sample_interval='31d',
                 resample_interval='1h', subreddit='cryptocurrency'):

        # call the init functions of the parent class
        super().__init__(collector_name='reddit-comments',
                         keyword=keyword,
                         start_date=start_date,
                         end_date=end_date,
                         sample_interval=sample_interval,
                         resample_interval=resample_interval)

        # defining class attributes
        self.reddit = PushshiftAPI()
        self.subreddit = subreddit

        # get the intervals that have already been queried previously and are
        # in the sqlite cache
        self.load_coverage()

    def define_cache_table(self):
        # initialize a metadata instance for the sqlite cache
        metadata = MetaData()

        # create a table to store the data that is downloaded
        self.cache_table = Table(self.collector_name,
                                 metadata,
                                 Column('keyword', String(32),
                                        primary_key=True),
                                 Column('id', Integer,
                                        primary_key=True),
                                 Column('subreddit', String(32)),
                                 Column('author', String(32)),
                                 Column('timestamp', DateTime),
                                 Column('text', Text))

        # create a table to store the coverage intervals
        self.coverage_table_name = '{0}-coverage'.format(self.collector_name)
        self.coverage_table = Table(self.coverage_table_name,
                                    metadata,
                                    Column('keyword', String(32),
                                           primary_key=True),
                                    Column('query_start', DateTime,
                                           primary_key=True),
                                    Column('query_end', DateTime))

        # add tables to the database
        metadata.create_all(self.cache_engine)

    def download_to_dataframe(self, interval_start, interval_end):
        # download comments over a given interval
        results_gen = self.reddit.search_comments(
            q=self.keyword,
            subreddit=self.subreddit,
            filter=['id', 'subreddit', 'author', 'created_utc', 'body'],
            after=int(interval_start.replace(
                tzinfo=timezone.utc).timestamp()),
            before=int(interval_end.replace(
                tzinfo=timezone.utc).timestamp()))

        # convert downlaoded data to a pandas dataframe
        cache_df = pd.DataFrame(results_gen)

        # if there is no data, return the empty dataframe as is
        if cache_df.empty:
            return cache_df

        # adding the keyword field to the dataframe
        cache_df['keyword'] = self.keyword

        # renaming fields for convenience
        cache_df = cache_df.rename(columns={'created_utc': 'timestamp',
                                            'body': 'text'})

        # convert the timestamps to datetime objects
        cache_df['timestamp'] = pd.to_datetime(cache_df['timestamp'], unit='s')

        # reorder the fields to be consistent with sqlite cache
        cache_df = cache_df[['keyword',
                             'id',
                             'subreddit',
                             'author',
                             'timestamp',
                             'text']]

        # return the downloaded data as a dataframe
        return cache_df

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

    def pre_cache_routine(self, interval_start, interval_end):
        # remove interval from cache if there is duplicate data
        query = self.interval_sql_query(interval_start, interval_end)
        query.delete(synchronize_session=False)
        self.session.commit()

    def post_cache_routine(self, interval_start, interval_end):
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

        # convert coverge list to dataframe
        self.coverage = [list(x) for x in merged_coverage]
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

    def is_cache_complete(self, interval_start, interval_end, cache_df):
        # test if the interval is contained within the bounds of any of the
        # coverage intervals
        for coverage_start, coverage_end in self.coverage:
            if (interval_start >= coverage_start and
                    interval_end <= coverage_end):
                return True
        return False

    def handle_download_error(self, interval_start, interval_end, error):
        # no error handling required as of now
        raise

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



        #
        #     bound = min(bound, self.end_date)
        #
        #     if min_existing_date and max_existing_date:
        #         if bound < min_existing_date or bound > max_existing_date:
        #             interval_bounds.append(bound)
        #     else:
        #             interval_bounds.append(bound)
        #
        # if max_existing_date and min_existing_date:
        #     interval_bounds.append(max_existing_date)
        #     interval_bounds.append(min_existing_date)
        #
        # interval_bounds.sort()
        #
        # start_intervals = interval_bounds[:-1]
        # end_intervals = interval_bounds[1:]
        #
        # intervals = [i for i in zip(start_intervals, end_intervals)]
        #
        # self.existing_interval = (min_existing_date, max_existing_date)





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

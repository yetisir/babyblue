from . import DataCollector
from psaw import PushshiftAPI
from datetime import datetime, timezone, timedelta
import pandas as pd
import time

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

        self.load_coverage()

    def define_cache_table(self):
        metadata = MetaData()
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

        self.coverage_table_name = '{0}-coverage'.format(self.collector_name)
        self.coverage_table = Table(self.coverage_table_name,
                                    metadata,
                                    Column('keyword', String(32),
                                           primary_key=True),
                                    Column('query_start', DateTime,
                                           primary_key=True),
                                    Column('query_end', DateTime))
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

    def load_coverage(self):
        query = self.session.query(self.coverage_table)
        query = query.filter((self.coverage_table.c.keyword == self.keyword))

        coverage_df = pd.read_sql(sql=query.statement,
                                  con=self.session.bind)

        if coverage_df.empty:
            coverage_df = pd.DataFrame(data=None,
                                       columns=coverage_df.columns,
                                       index=coverage_df.index)
        coverage = coverage_df[['query_start', 'query_end']].values.tolist()
        for i, interval in enumerate(coverage):
            for b, bound in enumerate(interval):
                coverage[i][b] = datetime.utcfromtimestamp(bound * 1e-9)
        self.coverage = coverage

    def pre_cache_routine(self, interval_start, interval_end):
        # TO DO: check for overlap between existing cache and recent query
        # remove partial interval from cache
        query = self.interval_sql_query(interval_start, interval_end)
        query.delete(synchronize_session=False)
        self.session.commit()

    def post_cache_routine(self, interval_start, interval_end):
        merged_coverage = []

        coverage = self.coverage
        coverage.append([interval_start, interval_end])
        coverage = sorted(coverage, key=lambda x: x[0])

        for higher in coverage:
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
                    merged_coverage.append(higher)

        self.coverage = [list(x) for x in merged_coverage]

        coverage_df = pd.DataFrame(self.coverage, columns=['query_start',
                                                           'query_end'])
        coverage_df.insert(0, 'keyword', self.keyword)

        query = self.session.query(self.coverage_table)
        query = query.filter((self.coverage_table.c.keyword == self.keyword))
        query.delete(synchronize_session=False)
        self.session.commit()

        coverage_df.to_sql(self.coverage_table_name, self.cache_engine,
                           if_exists='append', index=False)

    def is_cache_complete(self, interval_start, interval_end, cache_df):

        for coverage_start, coverage_end in self.coverage:
            if (interval_start >= coverage_start and
                    interval_end <= coverage_end):
                return True
        return False

    def handle_download_error(self, error):

        raise

    def download_intervals(self):

        num_intervals = ((self.end_date - self.start_date) //
                         self.sample_interval) + 1

        intervals = []
        for i in range(num_intervals):
            lower_bound = self.start_date + i * self.sample_interval
            upper_bound = lower_bound + self.sample_interval
            if upper_bound > datetime.utcnow():
                upper_bound = datetime.utcnow()
            interval = [lower_bound, upper_bound]

            for j, coverage in enumerate(self.coverage):
                overlap = max(timedelta(0), min(interval[1], coverage[1]) -
                              max(interval[0], coverage[0]))
                if overlap > timedelta(0):
                    if (interval[0] < coverage[1] and
                            interval[1] > coverage[1]):
                        interval[0] = coverage[1]
                    if (interval[1] > coverage[0] and
                            interval[0] < coverage[0]):
                        interval[1] = coverage[0]

            intervals.append(interval)




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

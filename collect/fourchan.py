from . import CommentCollector
from psaw import PushshiftAPI
from datetime import timezone
import pandas as pd


class FourChanComments(CommentCollector):
    def __init__(self, keyword, start_date, end_date, sample_interval='31d',
                 resample_interval='1h', board='biz'):

        # call the init functions of the parent class
        super().__init__(collector_name='reddit-comments',
                         keyword=keyword,
                         start_date=start_date,
                         end_date=end_date,
                         sample_interval=sample_interval,
                         resample_interval=resample_interval,
                         community_title='board')

        # defining class attributes
        self.reddit = PushshiftAPI()
        self.board = board

        # get the intervals that have already been queried previously and are
        # in the sqlite cache
        self.load_coverage()

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

    def handle_download_error(self, interval_start, interval_end, error):
        # no error handling required as of now
        raise

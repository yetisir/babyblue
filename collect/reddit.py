from . import CommentCollector
from psaw import PushshiftAPI
from datetime import timezone
import pandas as pd


class RedditComments(CommentCollector):
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

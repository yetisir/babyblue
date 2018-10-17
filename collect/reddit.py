from . import DataCollector
from psaw import PushshiftAPI
import datetime
import pandas as pd


class RedditComments(DataCollector):
    def __init__(self, keyword, start_date, end_date, category=0,
                 sample_interval='2d', resample_interval='1h',
                 subreddit=''):

        # call the init functions of the parent class
        super().__init__(collector_name='reddit-comments',
                         keyword=keyword,
                         start_date=start_date,
                         end_date=end_date,
                         sample_interval=sample_interval)

        # defining class attributes
        self.reddit = PushshiftAPI()
        self.subreddit = subreddit
        self.resample_interval = resample_interval

    def query(self):

        results_gen = self.reddit.search_comments(
            q=self.keyword,
            subreddit=self.subreddit,
            filter=['author', 'created_utc', 'body'],
            after=int(self.interval_start.replace(
                tzinfo=datetime.timezone.utc).timestamp()),
            before=int(self.interval_end.replace(
                tzinfo=datetime.timezone.utc).timestamp()))

        comment_list = []
        for comment in results_gen:
            comment_list.append(
                {'author': comment.author,
                 'datetime': datetime.datetime.utcfromtimestamp(
                     comment.created_utc),
                 'comment': comment.body,
                 '{0}_mentions'.format(
                     self.keyword): comment.body.lower().count(self.keyword),
                 '{0}_comments'.format(self.keyword): 1})

        comment_df = pd.DataFrame(comment_list)
        comment_df = comment_df.set_index('datetime')
        interval_df = comment_df.resample(self.resample_interval).sum()

        print(interval_df)
        if (datetime.datetime.utcnow() - interval_df.index[-1] <
                pd.to_timedelta(self.resample_interval)):
            interval_df = interval_df[:-1]
        print(interval_df)

        # return dataframe of interval
        return interval_df

    def handle_query_error(self, error):

        raise

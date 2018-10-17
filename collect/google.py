from . import DataCollector
from pytrends.request import TrendReq
import pandas as pd
import time


class GoogleTrends(DataCollector):
    def __init__(self, keyword, start_date, end_date, category=0,
                 sample_interval=2, overlap_interval=1, wait=1,
                 sleep=60, time_format='%Y-%m-%dT%H'):

        super().__init__(collector_name='google-trends',
                         keyword=keyword,
                         start_date=start_date,
                         end_date=end_date,
                         sample_interval=sample_interval,
                         overlap_interval=overlap_interval,
                         wait=wait,
                         time_format=time_format)

        self.sleep = sleep
        self.category = category
        self.pytrend = TrendReq()

    def query(self):
        timeframe = '{0} {1}'.format(self.interval_start_str,
                                     self.interval_end_str)
        self.pytrend.build_payload([self.keyword], cat=self.category,
                                   timeframe=timeframe)
        interval_df = self.pytrend.interest_over_time()
        interval_df = interval_df.shift(periods=1)
        print(interval_df.tail())
        interval_df['datetime'] = pd.to_datetime(interval_df.index)
        interval_df = interval_df.set_index('datetime')
        interval_df['isPartial'] = interval_df['isPartial'].astype(str)
        interval_df = interval_df.loc[
            interval_df.isPartial.str.contains('False')]
        interval_df = interval_df.drop('isPartial', axis='columns')
        return interval_df

    def handle_query_error(self, error):
        if str(error)[-4:-1] == '429':
            # TODO replace with more appropriate logging method
            print('QUERY LIMIT REACHED: waiting {0} '
                  'seconds'.format(self.sleep))
            time.sleep(self.sleep)
        else:
            # TODO replace with more appropriate logging method
            print('FAILED: ', self.keyword, self.interval_start,
                  self.interval_end)
            raise

    def get_dataframe(self):

        # return the dataframe with the collected data
        return self.keyword_df

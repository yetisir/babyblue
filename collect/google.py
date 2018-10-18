from . import DataCollector
from pytrends.request import TrendReq
import pandas as pd
import time


class GoogleTrends(DataCollector):
    def __init__(self, keyword, start_date, end_date, category=0,
                 sample_interval='2d', overlap_interval='1d', wait=1,
                 sleep=60, time_format='%Y-%m-%dT%H'):

        # call the init functions of the parent class
        super().__init__(collector_name='google-trends',
                         keyword=keyword,
                         start_date=start_date,
                         end_date=end_date,
                         sample_interval=sample_interval,
                         overlap_interval=overlap_interval,
                         wait=wait,
                         time_format=time_format)

        # defining class attributes
        self.sleep = sleep
        self.category = category
        self.pytrend = TrendReq()

    def query(self):
        # send request to google for the trend data
        timeframe = '{0} {1}'.format(self.interval_start_str,
                                     self.interval_end_str)
        self.pytrend.build_payload([self.keyword], cat=self.category,
                                   timeframe=timeframe)
        interval_df = self.pytrend.interest_over_time()

        # convert index to datetime index
        interval_df['datetime'] = pd.to_datetime(interval_df.index)
        interval_df = interval_df.set_index('datetime')

        # drop partial data record
        interval_df['isPartial'] = interval_df['isPartial'].astype(str)
        interval_df = interval_df.loc[
            interval_df.isPartial.str.contains('False')]

        # drop row identifying if the data is partial or not
        interval_df = interval_df.drop('isPartial', axis='columns')

        # return dataframe of interval
        return interval_df

    def handle_query_error(self, error):

        # catch blocked ip error. this may happen if there are too many request
        # over a short period of time. this is sometimes resolved witha one
        # minute buffer
        if str(error)[-4:-1] == '429':
            # TODO replace with more appropriate logging method
            print('QUERY LIMIT REACHED: waiting {0} '
                  'seconds'.format(self.sleep))
            time.sleep(self.sleep)
            self.query_interval()
        else:
            raise

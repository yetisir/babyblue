import datetime
import os
import pickle
import time


class DataCollector(object):
    def __init__(self, collector_name, keyword, start_date, end_date,
                 sample_interval, overlap_interval=0,
                 time_format='%Y-%m-%dT%H'):

        self.collector_name = collector_name
        self.keyword = keyword
        self.time_format = time_format

        # convert dates and times to datetime objects
        self.start_date = datetime.datetime(start_date)
        self.end_date = datetime.datetime(end_date)
        self.sample_interval = datetime.timedelta(days=sample_interval)
        self.overlap_interval = datetime.timedelta(days=overlap_interval)

        # defining query limits based on the epoch and interval so that they
        # are consistent regardless of the specified start date
        epoch = datetime.datetime.utcfromtimestamp(0)

        # push back the start date so that there is an natural number of
        # intervals between the epoch and the start
        self.query_start = (self.start_date - (self.start_date - epoch) %
                            self.sample_interval)

        # push forward the end date so that there are a natural number of
        # intervals between the start and end dates
        self.query_end = (self.end_date - (self.end_date - self.query_start) %
                          self.sample_interval + self.sample_interval)

        # determine the total number of intervals to be queried
        self.num_intervals = ((self.query_end - self.query_start) //
                              self.sample_interval)

        # initialize empty dataframe to store all collected data
        self.keyword_df = pd.DataFrame()

    def query_data(self):
        for i in range(num_intervals):
            self.interval_start = (query_start + i*sample_interval -
                                   overlap_interval)
            self.interval_end = (query_start + (i+1)*sample_interval)

            self.interval_start_str = self.interval_start.strftime(
                self.time_format)
            self.interval_end_str = self.interval_end.strftime(
                self.time_format)

            complete_interval = True
            if os.path.isfile(cached_file_path):
                interval_df = self.load_cache()
                if interval_df.index[-1] < self.interval_end:
                    complete_interval = False

            if not complete_interval or not os.path.isfile(cached_file_path):
                try:
                    print('DOWNLOADING: ', self.keyword, self.interval_start,
                          self.interval_end)
                    self.query()
                    self.dump_cache(start_date, end_date)
                    time.sleep(wait)
                except Exception as error:
                    self.handle_query_error(error)

            if self.overlap_interval and len(self.keyword_df) > 0:
                self.merge_overlap()

            coin_df = coin_df.append(self.interval_df)

    def query(self): #Class Specific
        pytrend.build_payload([coin], cat=cat, timeframe=timeframe)
        interval_df = pytrend.interest_over_time()
        interval_df['datetime'] = pd.to_datetime(interval_df.index)
        interval_df = interval_df.set_index('datetime')

    def handle_query_error(self, error): #Class Sepcific
        if int(str(error)[-4:-1]) == 429:
            print('QUERY LIMIT REACHED: waiting {0} '
                  'seconds'.format(sleeptime))
            time.sleep(sleeptime)
        else:
            print('FAILED: ', coin, timeframe, error)

    def cache_file_path(self):
        # compile cache file name
        fmt_str = '{collector}_{keyword}_{start_date}_to_{end_date}.pkl'
        cached_file_name = fmt_str.format(keyword=self.keyword,
                                          start_date=self.interval_start_str,
                                          end_date=self.interval_end_str)

        # return the full path to the cache file
        return os.path.join(os.path.pardir, 'cache', 'collector',
                            cached_file_name)

    def load_cache(self, start_date, end_date):
        # get cache file name
        cache_file = self.cache_file_path()

        # load serialized data from cache
        with open(cache_file, 'rb') as file:
            print('LOADING FROM CACHE: ', self.keyword, self.interval_start,
                  self.interval_end)
            data = pickle.load(file)

        # return unpickled dataframe for interval
        return data

    def dump_cache(self, start_date, end_date):
        # get cache file name
        cache_file = self.cache_file_path()

        # pickle interval dataframe
        with open(cache_file, 'wb') as file:
            pickle.dump(self.interval_df, file)

    def merge_overlap(self):
        df_time_range = self.keyword_df.index[-1] - self.keyword_df.index[0]

        if df_time_range > self.overlap_interval:
            overlap_start = self.interval_start
            overlap_end = self.interval_start + self.overlap_interval
        else:
            overlap_start = self.keyword_df.index[0]
            overlap_end = self.keyword_df.index[-1]

        previous_interval_overlap = self.keyword_df.loc[overlap_start:,
                                                        self.keyword]
        current_interval_overlap = self.interval_df.loc[:overlap_end,
                                                        self.keyword]
        previous_average = previous_interval_overlap.mean()
        current_average = current_interval_overlap.mean()

        self.interval_df.loc[:, self.keyword] = (
            self.interval_df[self.keyword] * (previous_average /
                                              current_average))
        self.interval_df = self.interval_df.loc[(self.interval_start +
                                                 self.overlap_interval):]

import datetime
import os
import pickle
import time
import pandas as pd


class DataCollector(object):
    def __init__(self, collector_name, keyword, start_date, end_date,
                 sample_interval, overlap_interval=0, wait=1,
                 time_format='%Y-%m-%dT%H'):

        self.collector_name = collector_name
        self.keyword = keyword
        self.time_format = time_format
        self.wait = wait
        self.start_date = start_date
        self.end_date = end_date

        # convert times to datetime timedelta objects
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
        # loop through each interval and compile a dataset
        for i in range(self.num_intervals):
            # determing the bounding datetimes for the interval
            self.interval_start = (self.query_start + i*self.sample_interval -
                                   self.overlap_interval)
            self.interval_end = (self.query_start + (i+1)*self.sample_interval)

            # convert the interval bounds to strings
            self.interval_start_str = self.interval_start.strftime(
                self.time_format)
            self.interval_end_str = self.interval_end.strftime(
                self.time_format)

            # check to see if the interval has already been downloaded
            complete_interval = True
            cache_file = self.cache_file_path()
            if os.path.isfile(cache_file):
                self.interval_df = self.load_cache()
                if self.interval_df.index[-1] < self.interval_end:
                    complete_interval = False

                    interval_df_timedelta = (self.interval_df.index[-1] -
                                             self.interval_df.index[-2])
                    current_timedelta = (datetime.datetime.utcnow() -
                                         self.interval_df.index[-1])
                    print(datetime.datetime.utcnow())
                    print(self.interval_df.index[-1])
                    print(current_timedelta)
                    print(interval_df_timedelta)
                    if current_timedelta < interval_df_timedelta:
                        complete_interval = True

            # download the interval again if it does not exist or is incomplete
            if not complete_interval or not os.path.isfile(cache_file):
                try:
                    # TODO replace with more appropriate logging method
                    print('DOWNLOADING: ', self.keyword, self.interval_start,
                          self.interval_end)

                    # query method is defined by the child class
                    self.interval_df = self.query()
                    self.dump_cache()
                    time.sleep(self.wait)

                # catch the exception and pass to child class
                except Exception as error:
                    # handle_query_error method is defined by the child class
                    self.handle_query_error(error)

            # merge data if specified and not the first interval
            if self.overlap_interval and len(self.keyword_df):
                self.merge_overlap()
            # concatenate the interval to the dataframe
            if len(self.keyword_df):
                self.keyword_df = self.keyword_df.append(self.interval_df,
                                                         sort=False)
            else:
                self.keyword_df = self.interval_df

        # chop dataframe to originally requested size
        self.keyword_df = self.keyword_df.loc[self.start_date:self.end_date]

    def cache_file_path(self):
        # compile cache file name
        fmt_str = '{collector}_{keyword}_{start_date}_to_{end_date}.pkl'
        cached_file_name = fmt_str.format(keyword=self.keyword,
                                          start_date=self.interval_start_str,
                                          end_date=self.interval_end_str,
                                          collector=self.collector_name)

        # return the full path to the cache file
        return os.path.abspath(os.path.join(os.path.dirname(__file__),
                                            os.path.pardir,
                                            'cache',
                                            self.collector_name,
                                            cached_file_name))

    def load_cache(self):
        # get cache file name
        cache_file = self.cache_file_path()

        # load serialized data from cache
        with open(cache_file, 'rb') as file:
            # TODO replace with more appropriate logging method
            print('LOADING FROM CACHE: ', self.keyword, self.interval_start,
                  self.interval_end)
            data = pickle.load(file)
            data.columns = map(str.lower, data.columns)

        # return unpickled dataframe for interval
        return data

    def dump_cache(self):
        # get cache file name
        cache_file = self.cache_file_path()

        # ensure the directory exists
        os.makedirs(os.path.dirname(cache_file), exist_ok=True)

        # pickle interval dataframe
        with open(cache_file, 'wb') as file:
            pickle.dump(self.interval_df, file)

    def merge_overlap(self):

        # get timedelta of existing data
        df_time_range = self.keyword_df.index[-1] - self.keyword_df.index[0]

        # select all of overalp data if it exists
        if df_time_range > self.overlap_interval:
            overlap_start = self.interval_start
            overlap_end = self.interval_start + self.overlap_interval

        # otherwise only select range of existing data to merge
        else:
            overlap_start = self.keyword_df.index[0]
            overlap_end = self.keyword_df.index[-1]

        # get overlapping series
        previous_interval_overlap = self.keyword_df.loc[overlap_start:,
                                                        self.keyword]
        current_interval_overlap = self.interval_df.loc[:overlap_end,
                                                        self.keyword]

        # get averages of overalpping series
        previous_average = previous_interval_overlap.mean()
        current_average = current_interval_overlap.mean()

        # normalize new interval based on the overlap with the old interval
        self.interval_df.loc[:, self.keyword] = (
            self.interval_df[self.keyword] * (previous_average /
                                              current_average))

        # chop off overlapping data from new interval
        self.interval_df = self.interval_df.loc[(self.interval_start +
                                                 self.overlap_interval):]
        self.interval_df = self.interval_df.loc[self.interval_df.index[1]:]

    def get_dataframe(self):

        # return the dataframe with the collected data
        return self.keyword_df

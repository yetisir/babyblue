import ccxt
from . import SequentialDataCollector
from sqlalchemy import Column, Float, DateTime, String
import pandas as pd
from datetime import datetime


class Binance(SequentialDataCollector):
    def __init__(self, keyword, start_date, end_date, sample_interval='20d',
                 data_resolution='1h'):

        # current limit of 500 data points

        super().__init__(collector_name='binance',
                         keyword=keyword,
                         start_date=start_date,
                         end_date=end_date,
                         sample_interval=sample_interval,
                         data_resolution=data_resolution)

        self.data_resolution_string = data_resolution
        self.binance = ccxt.binance()

    def define_cache_table(self, Base):
        cache = {'__tablename__': self.collector_name,
                 'keyword': Column('keyword', String(32),
                                   primary_key=True),
                 'candle_start': Column(DateTime,
                                        primary_key=True),
                 'candle_interval': Column(DateTime,
                                           primary_key=True),
                 'open': Column(Float),
                 'high': Column(Float),
                 'low': Column(Float),
                 'close': Column(Float),
                 'volume': Column(Float)}

        self.cache_table = type('Cache', (Base, ), cache)

    def download_to_dataframe(self, interval_start, interval_end):
        since = int(interval_start.timestamp() * 1e3)

        limit = (interval_end - interval_start) // self.data_resolution
        pair = '{0}/BTC'.format(self.keyword.upper())
        data = self.binance.fetch_ohlcv(pair, since=since, limit=limit,
                                        timeframe=self.data_resolution_string)

        interval_df = pd.DataFrame(data, columns=['candle_start',  # end?
                                                  'open',
                                                  'high',
                                                  'low',
                                                  'close',
                                                  'volume'])

        interval_df['candle_interval'] = self.data_resolution
        interval_df['keyword'] = self.keyword
        interval_df['candle_start'] = pd.to_datetime(
            interval_df['candle_start'], unit='ms')

        interval_df = interval_df[['keyword',
                                   'candle_start',  # end?
                                   'candle_interval',
                                   'open',
                                   'high',
                                   'low',
                                   'close',
                                   'volume']]

        return interval_df

    def handle_download_error(self, interval_start, interval_end, error):
        if 'No market symbol' in str(error):
            return

    def dataframe_to_sql(self, cache_df):
        cache_df['candle_interval'] = cache_df['candle_interval'] + self.epoch

        self.merge_dataframe_into_table(cache_df, self.collector_name,
                                        ['keyword', 'candle_start',
                                         'candle_interval'])

    def sql_to_dataframe(self, interval_start, interval_end):
        # load data from cache
        query = self.interval_sql_query(interval_start, interval_end)
        query = query.order_by('candle_start')
        cache_df = pd.read_sql(sql=query.statement, con=self.session.bind)

        # convert back to timedeltas
        cache_df['candle_interval'] = cache_df['candle_interval'] - self.epoch

        # return cached data as dataframe
        return cache_df

    def interval_sql_query(self, interval_start, interval_end):
        # generate the sql query for retrieving cached data
        query = self.session.query(self.cache_table)
        query = query.filter(
            (self.cache_table.keyword == self.keyword),
            (self.cache_table.candle_start >= interval_start),
            (self.cache_table.candle_start < interval_end))

        # return the cache query
        return query

    def post_cache_routine(self, interval_start, interval_end):
        pass

    def pre_cache_routine(self, interval_start, interval_end):
        pass

    def process_raw_data(self, data_df):
        data_df = data_df[['candle_start',
                           'open']]

        data_df = data_df.set_index('candle_start')
        data_df.index.names = ['data_start']  # TODO: check to see if shifted
        # data_df = data_df.resample(self.data_resolution).asfreq()
        return data_df

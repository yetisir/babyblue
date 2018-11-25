from . import DataCollector
from sqlalchemy import Column, String, Text
import requests
import pandas as pd
from datetime import datetime
import json


class CoinMarketCapMetaData(DataCollector):
    def __init__(self, symbol, api_key, force_update=False):

        self.coverage_interval = datetime.utcfromtimestamp(0)
        epoch = datetime.utcfromtimestamp(0)
        now = datetime.utcnow()
        super().__init__(collector_name='coinmarketcap',
                         keyword=symbol,
                         start_date=epoch,
                         end_date=now,
                         sample_interval=now - epoch,
                         data_resolution='1d')

        url = 'https://pro-api.coinmarketcap.com/v1'
        endpoint = 'cryptocurrency/info?symbol={0}'.format(symbol)
        self.api_url = '{0}/{1}'.format(url, endpoint)

        self.coinmarketcap = requests.Session()
        self.coinmarketcap.headers.update({
            'Content-Type': 'application/json',
            'X-CMC_PRO_API_KEY': api_key})

    def define_cache_table(self, Base):
        cache = {'__tablename__': self.collector_name,
                 'symbol': Column(String(32),
                                  primary_key=True),
                 'name': Column(String(32)),
                 'category': Column(String(32)),
                 'logo': Column(Text),
                 'website': Column(Text),
                 'source_code': Column(Text),
                 'message_board': Column(Text),
                 'announcement': Column(Text),
                 'reddit': Column(Text),
                 'twitter': Column(Text)}

        self.cache_table = type('Cache', (Base, ), cache)

    def download_to_dataframe(self, interval_start, interval_end):
        response = self.coinmarketcap.get(self.api_url)
        data = json.loads(response.text)['data'][self.keyword.upper()]

        interval_df = pd.DataFrame(
            [[self.keyword,
              data['name'],
              data['category'],
              data['logo'],
              next(iter(data['urls']['website']), None),
              next(iter(data['urls']['source_code']), None),
              next(iter(data['urls']['message_board']), None),
              next(iter(data['urls']['announcement']), None),
              next(iter(data['urls']['reddit']), None),
              next(iter(data['urls']['twitter']), None)]],
            columns=['symbol',
                     'name',
                     'category',
                     'logo',
                     'website',
                     'source_code',
                     'message_board',
                     'announcement',
                     'reddit',
                     'twitter'])

        return interval_df

    def handle_download_error(self, interval_start, interval_end, error):
        raise

    def dataframe_to_sql(self, cache_df):
        cache_df.to_sql(self.collector_name, self.cache_engine,
                        if_exists='append', index=False)

    def sql_to_dataframe(self, interval_start, interval_end):
        query = self.interval_sql_query(interval_start, interval_end)
        cache_df = pd.read_sql(sql=query.statement, con=self.session.bind)
        return cache_df

    def interval_sql_query(self, interval_start, interval_end):
        # generate the sql query for retrieving cached data
        query = self.session.query(self.cache_table)
        query = query.filter(
            (self.cache_table.symbol == self.keyword))

        # return the cache query
        return query

    def post_cache_routine(self, interval_start, interval_end):
        pass

    def pre_cache_routine(self, interval_start, interval_end):
        pass

    def process_raw_data(self, data_df):
        return data_df

    def download_intervals(self):
        return ((self.start_date, self.end_date),)

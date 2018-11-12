import ccxt
from . import ExchangeCollector

class Binance(ExchangeCollector):
    def __init__(self):
        pass

    def define_cache_table(self, Base):
        pass

    def download_to_dataframe(self, interval_start, interval_end):
        pass

    def handle_download_error(self, interval_start, interval_end):
        pass

    def dataframe_to_sql(self, cache_df):
        pass

    def sql_to_dataframe(self, interval_start, interval_end):
        pass

    def interval_sql_query(self, interval_start, interval_end):
        pass

    def post_cache_routine(self, interval_start, interval_end):
        pass

    def pre_cache_routine(self, interval_start, interval_end):
        pass

    def download_intervals(self):
        pass

    def process_raw_data(self, data_df):
        pass

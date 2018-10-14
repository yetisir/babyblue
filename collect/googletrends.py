from pytrends.request import TrendReq
import pandas as pd
import datetime
import matplotlib.pyplot as plt
import os
import pickle


def load_data(coin_list, from_date, to_date):
    google_trends_dfs = []

    for coin_ticker, coin_name in coin_list.items():

        coin_trends = load_google_trends(coin=coin_name,
                                         from_date=from_date,
                                         to_date=to_date)
        if any(coin_trends):
            coin_trends = coin_trends.drop('isPartial', axis='columns')
            google_trends_dfs.append(coin_trends)

    return pd.concat(google_trends_dfs, axis='columns')


def load_google_trends(coin, from_date, to_date, sample_interval=4,
                       overlap_interval=1, cat=7, sleeptime=60):

    sample_interval = int(sample_interval)
    if sample_interval + overlap_interval > 7:
        raise ValueError('Google API limits require overlap_interval + '
                         'sample_interval to be no more than 7')

    sample_interval = datetime.timedelta(days=sample_interval)
    overlap_interval = datetime.timedelta(days=overlap_interval)

    epoch = datetime.datetime.utcfromtimestamp(0)
    query_start = (from_date - (from_date - epoch) % sample_interval)
    query_end = (to_date - (to_date - query_start) % sample_interval +
                 sample_interval)

    num_intervals = (query_end - query_start) // sample_interval
    pytrend = TrendReq()
    coin_df = pd.DataFrame()

    for i in range(num_intervals):
        interval_start = query_start + i*sample_interval - overlap_interval
        interval_end = query_start + (i+1)*sample_interval
        time_format = '%Y-%m-%dT%H'

        interval_start = interval_start.strftime(time_format)
        interval_end = interval_end.strftime(time_format)
        timeframe = interval_start + ' ' + interval_end

        cached_file_name = ('google-trends_{coin}_{from_date}_to_'
                            '{to_date}.pkl').format(coin=coin,
                                                    from_date=interval_start,
                                                    to_date=interval_end)

        cached_file_path = os.path.join(os.path.pardir, 'cache',
                                        cached_file_name)
        if os.path.isfile(cached_file_path):
            with open(cached_file_path, 'rb') as file:
                interval_df = pickle.load(file)
        else:
            try:
                print('DOWNLOADING: ', coin, timeframe)
                pytrend.build_payload([coin], cat=cat, timeframe=timeframe)
                interval_df = pytrend.interest_over_time()

                with open(cached_file_path, 'wb') as file:
                    pickle.dump(interval_df, file)
            except:
                print('FAILED: ', coin, timeframe)
                pass

        # TODO: use overlap interval for slicing instead of hard coded.
        if len(coin_df) > 24:
            last_24_hrs = coin_df.iloc[-24:]
            first_24_hrs = interval_df.iloc[:24]
            last_average = last_24_hrs.mean()
            first_average = first_24_hrs.mean()
            interval_df.loc[:, coin] = (interval_df.loc[:, coin] *
                                        last_average / first_average)

        coin_df = coin_df.append(interval_df)

    return coin_df

if __name__ == '__main__':
    coin_list = {'BTC': 'bitcoin',
                 'ETH': 'ethereum',
                 'XRP': 'ripple',
                 'BCH': 'bitcoin cash',
                 'EOS': 'eos'}

    """                 'XLM': 'stellar',
                 'LTC': 'litecoin',
                 'ADA': 'cardano',
                 'XMR': 'monero',
                 'TRX': 'tron',
                 'MIOTA': 'iota',
                 'DASH': 'dash',
                 'BNB': 'binance',
                 'NEO': 'neo'}"""

    start_date = datetime.datetime(year=2017, month=1, day=1, hour=0)
    end_date = datetime.datetime.today()

    data = load_data(coin_list, start_date, end_date)
    print(data.to_string())
    plt.plot(data)
    plt.show()

from collect.google import GoogleTrends
import datetime
import pandas as pd
import matplotlib.pyplot as plt


def load_data(coin_list, start_date, end_date):
    google_trends_dfs = []

    for coin_name in coin_list:

        gtrend = GoogleTrends(keyword=coin_name,
                              start_date=start_date,
                              end_date=end_date)
        gtrend.query_data()
        gtrend_data = gtrend.get_dataframe()

        if any(gtrend_data):
            google_trends_dfs.append(gtrend_data)

    return pd.concat(google_trends_dfs, axis='columns')


if __name__ == '__main__':
    coin_list = {'BTC': 'bitcoin',
                 'ETH': 'ethereum',
                 'XRP': 'ripple',
                 'BCH': 'bitcoin cash',
                 'EOS': 'eos'}
                 # 'XLM': 'stellar',
                 # 'LTC': 'litecoin',
                 # 'ADA': 'cardano',
                 # # 'XMR': 'monero',
                 # 'TRX': 'tron',
                 # 'MIOTA': 'iota',
                 # 'DASH': 'dash',
                 # 'BNB': 'binance',
                 # 'NEO': 'neo'}

    coin_list = coin_list.values()

    start_date = datetime.datetime(year=2018, month=9, day=1, hour=0)
    end_date = datetime.datetime.today()

    data = load_data(coin_list, start_date, end_date)
    plt.plot(data)
    plt.legend(coin_list)
    plt.show()

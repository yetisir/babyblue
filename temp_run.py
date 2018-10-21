from collect.assimilate import DataAssimilator
import datetime
import matplotlib.pyplot as plt

if __name__ == '__main__':
    coin_list = {
                 'BTC': 'bitcoin',
                 'ETH': 'ethereum',
                 'XRP': 'ripple',
                 'BCH': 'bitcoin cash',
                 'EOS': 'eos',
                 'XLM': 'stellar',
                 # 'LTC': 'litecoin',
                 # 'ADA': 'cardano',
                 # 'XMR': 'monero',
                 # 'TRX': 'tron',
                 # 'MIOTA': 'iota',
                 # 'DASH': 'dash',
                 # 'BNB': 'binance',
                 # 'NEO': 'neo'
                 }

    coin_list = list(coin_list.values())

    start_date = datetime.datetime(year=2018, month=1, day=1, hour=0)
    end_date = datetime.datetime.utcnow()

    assimilator = DataAssimilator(coin_list, start_date, end_date)
    assimilator.add_google_trends()
    assimilator.add_reddit_comments()
    data = assimilator.get_data()

    plt.plot(data)
    plt.legend(data.columns)
    plt.show()

from collect.assimilate import DataAssimilator
import datetime
import plotly.offline as py
import plotly.graph_objs as go
import cufflinks as cf

if __name__ == '__main__':

    cf.go_offline()

    coin_list = {
                 # 'BTC': 'bitcoin',
                 # 'ETH': 'ethereum',
                 # 'XRP': 'ripple',
                 # 'BCH': 'bitcoin cash',
                 'EOS': 'eos',
                 # 'XLM': 'stellar',
                 # 'LTC': 'litecoin',
                 # 'ADA': 'cardano',
                 # 'XMR': 'monero',
                 # 'TRX': 'tron',
                 # 'MIOTA': 'iota',
                 # 'DASH': 'dash',
                 # 'BNB': 'binance',
                 # 'NEO': 'neo',
                 # 'ETC': 'ethereum classic'
                 }

    coin_list = list(coin_list.keys()) + list(coin_list.values())

    start_date = datetime.datetime(year=2017, month=6, day=1, hour=0)
    end_date = datetime.datetime.utcnow()

    assimilator = DataAssimilator(coin_list, start_date, end_date)
    assimilator.add_google_trends()
    assimilator.add_reddit_comments()
    assimilator.add_fourchan_comments()
    data = assimilator.get_data()

    layout = go.Layout(showlegend=True)

    file = py.plot(data.iplot(asFigure=True),
                   filename='plot.html', auto_open=False)
    print(file)

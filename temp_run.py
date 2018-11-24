from collect.assimilate import DataAssimilator
from collect.coinmarketcap import CoinMarketCapMetaData
import datetime
import plotly.offline as py
import plotly.graph_objs as go
import cufflinks as cf
import json


class Coin(object):

    def __init__(self, symbol):
        self.symbol = symbol
        with open('config.json') as config_file:
            config = json.load(config_file)

        data_df = CoinMarketCapMetaData(
            symbol, config['coinmarketcap_api_key']).compile()
        data = {col: data_df.iloc[0][i]
                for i, col in enumerate(data_df.columns)}

        self.name = data['name']
        self.category = data['category']
        self.logo = data['logo']
        self.website = data['website']
        self.source_code = data['source_code']
        self.message_board = data['message_board']
        self.announcement = data['announcement']
        self.reddit = data['reddit']
        self.twitter = data['twitter']

        print(self.source_code)
        print(self.announcement)


if __name__ == '__main__':

    cf.go_offline()

    start_date = datetime.datetime(year=2018, month=5, day=1, hour=0)
    end_date = datetime.datetime.utcnow()

    assimilator = DataAssimilator(Coin('LINK'), start_date, end_date)
    assimilator.add_google_trends()
    assimilator.add_reddit_comments()
    assimilator.add_fourchan_comments()
    assimilator.add_binance_exchange()
    data = assimilator.get_data()

    layout = go.Layout(showlegend=True)

    binance_labels = [c for c in data.columns if 'binance' in c]

    file = py.plot(data.iplot(asFigure=True, secondary_y=binance_labels),
                   filename='plot.html', auto_open=False)
    print(file)


# future_data_sources:
#   github repositories
#   blockchain data
#   bitcointalk forums
#   alternate subreddits
#   news articles

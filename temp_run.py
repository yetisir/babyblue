from collect.assimilate import DataAssimilator
import datetime
import plotly.offline as py
import plotly.graph_objs as go
import cufflinks as cf
from collect.filters import NotchFilter, GaussianFilter

if __name__ == '__main__':

    cf.go_offline()

    start_date = datetime.datetime(year=2018, month=5, day=1, hour=0)
    end_date = datetime.datetime.utcnow()

    assimilator = DataAssimilator(start_date, end_date, 'LINK')
    assimilator.add_google_trends(filters=[NotchFilter()])
    assimilator.add_reddit_comments(filters=[GaussianFilter(),
                                             NotchFilter()])
    assimilator.add_fourchan_comments(filters=[GaussianFilter(),
                                               NotchFilter()])
    assimilator.add_binance_exchange()
    data = assimilator.get_dataframe()
    plots = assimilator.get_plots()

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

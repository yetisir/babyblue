from collect.assimilate import DataAssimilator
import datetime
import plotly.offline as py
import plotly.graph_objs as go
import cufflinks as cf
from collect.filters import NotchFilter, GaussianFilter, DailyAverage
# from plot import dashboard


if __name__ == '__main__':

    cf.go_offline()

    start_date = datetime.datetime(year=2016, month=1, day=1, hour=0)
    end_date = datetime.datetime.utcnow()

    assimilator = DataAssimilator(start_date, end_date, 'XRP')
    # gtrends_columns = assimilator.add_google_trends(filters=[GaussianFilter(),
    #                                        NotchFilter()])
    reddit_columns = assimilator.add_reddit_comments(filters=[DailyAverage()])
    fourchan_columns = assimilator.add_fourchan_comments(filters=[DailyAverage()])
    binance_columns = assimilator.add_binance_exchange()

    data = assimilator.get_dataframe()
    plots = assimilator.get_plots()

    data['total_comments'] = 0
    for column in (reddit_columns + fourchan_columns):
        data['total_comments'] = data['total_comments'] + data[column]

    layout = go.Layout(showlegend=True)

    # binance_labels = [c for c in data.columns if 'binance' in c]
    file = py.plot(data[binance_columns+['total_comments']].iplot(asFigure=True, secondary_y=binance_columns),
                   filename='plot.html', auto_open=False)
    # print(file)

    # app = dashboard.spawn(plots)
    # app.run_server(debug=True)


# future_data_sources:
#   github repositories
#   blockchain data
#   bitcointalk forums
#   alternate subreddits
#   news articles

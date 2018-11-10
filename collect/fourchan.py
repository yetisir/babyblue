from . import CommentCollector
from psaw import PushshiftAPI
import pandas as pd

from datetime import datetime
from bs4 import BeautifulSoup
import requests


class FourChanCommentCollector(CommentCollector):
    def __init__(self, keyword, start_date, end_date, sample_interval='31d',
                 data_resolution='1h', board='biz'):

        # call the init functions of the parent class
        super().__init__(collector_name='fourchan',
                         keyword=keyword,
                         start_date=start_date,
                         end_date=end_date,
                         sample_interval=sample_interval,
                         data_resolution=data_resolution,
                         community_title='board')

        # defining class attributes
        self.board = board
        self.base_url = 'https://warosu.org/{board}/?'.format(board=self.board)

    def download_to_dataframe(self, interval_start, interval_end):
        args = {'task': 'search2',
                'search_text': self.keyword,
                'search_datefrom': interval_start,
                'search_dateto': interval_end,
                'offset': 0}

        text_list = []
        id_list = []
        time_list = []

        while 1:
            url = self.base_url

            for arg, value in args.items():
                url = '{url}{arg}={value}&'.format(url=url, arg=arg,
                                                   value=value)

            page = requests.get(url)

            soup = BeautifulSoup(page.content, 'html.parser')

            comments = soup.find_all('blockquote')
            for comment_text in comments:
                comment = comment_text.parent

                id = comment.attrs['id']
                time = comment.find(class_='posttime').attrs['title']
                time = datetime.utcfromtimestamp(int(time)/1000.0)
                text = comment.find('blockquote')
                [t.extract() for t in text('a')]
                text = text.get_text()

                text_list.append(text)
                id_list.append(id)
                time_list.append(time)

            num_comments = len(list(comments))

            if num_comments == 0:
                break
            else:
                args['offset'] += num_comments

        cache_df = pd.DataFrame(list(zip(text_list, id_list, time_list)),
                                columns=['text', 'id', 'timestamp'])
        cache_df['keyword'] = self.keyword
        cache_df['board'] = self.board
        cache_df['author'] = 'anon'
        # reorder the fields to be consistent with sqlite cache
        cache_df = cache_df[['keyword',
                             'id',
                             'board',
                             'author',
                             'timestamp',
                             'text']]

        # return the downloaded data as a dataframe
        return cache_df

    def handle_download_error(self, interval_start, interval_end, error):
        # no error handling required as of now
        raise

import praw
from psaw import PushshiftAPI


# reddit = praw.Reddit(client_id='MqS_fWc4vXaeuQ',
#                      client_secret='wIJ49cBTzxb3yN9qt1JvszgYkoQ',
#                      user_agent='windows:babyblue:0.1.1')
#
# for submission in reddit.subreddit('bitcoin').new(limit=10):
#     print(submission.title)
#     print(vars(submission))
#

import requests
f = requests.get('https://www.reddit.com/r/bitcoin/search.json?q=oop')
print(f.content)
f = requests.get('https://api.pushshift.io/reddit/search/submission/?subreddit=cryptocurrency&size=500')

https://api.pushshift.io/reddit/submission/comment_ids/6uey5x

https://api.pushshift.io/reddit/comment/search?ids=dlrezc8,dlrawgw,dlrhbkq

https://www.reddit.com/r/funny/comments.json?limit=100
# submission = reddit.submission(id='3g1jfi')
# submission.comments.replace_more(limit=None)
#
# for comment in submission.comments.list():
#     print(comment.body)


from scrapy import Spider
from crawl.items import Thread, Comment
from datetime import datetime
from scrapy.conf import settings
import pymongo
import json
from json import JSONDecodeError


class RedditSpider(Spider):
    name = 'reddit'
    target_database = 'reddit'
    allowed_domains = ['reddit.com']
    start_urls_fmt = [
        'https://www.reddit.com/r/{subreddit}/new.json?limit=100',
        'https://www.reddit.com/r/{subreddit}/comments.json?limit=100',
    ]

    boards_to_crawl = [
        'cryptocurrency',
    ]

    start_urls = [start_url.format(board) for (
        start_url in start_urls_fmt for board in boards_to_crawl)]

    def __init__(self):
        self.open_mongodb()

    def __del__(self):
        self.close_mongodb()

    def parse(self, response):
        response_data = json.loads(response.text)

        if response_data is None:
            for board in self.boards_to_crawl:
                board_url = self.board_api_url.format(
                    base_url=self.start_urls[0], board=board, page=1)
                yield response.follow(
                    board_url,
                    callback=self.parse,
                    meta={'board': board, 'page': 1}
                )

        else:
            board = response.meta.get('board')
            for thread_id, thread_data in response_data.items():
                thread, comments = self.parse_thread(response, thread_data)
                yield thread

                for comment in comments:
                    yield comment

                if len(comments) == 0:
                    yield response.follow(
                        thread['url'],
                        callback=self.parse,
                        meta={'board': board}
                    )

            current_page = response.meta.get('page')
            if current_page is not None and len(response_data) > 0:
                next_page = current_page + 1
                board_url = self.board_api_url.format(
                    base_url=self.start_urls[0], board=board, page=next_page)

                yield response.follow(
                    board_url,
                    callback=self.parse,
                    meta={'board': board, 'page': next_page}
                )

    def parse_thread(self, response, thread):
        item = Thread()
        item['title'] = thread['op']['title']
        if item['title'] is None:
            item['title'] = ''

        item['id'] = thread['op']['thread_num']

        item['author'] = thread['op']['poster_hash']

        item['board'] = response.meta.get('board')

        item['url'] = self.thread_api_url.format(
            base_url=self.start_urls[0], board=item['board'],
            thread=item['id'])

        item['last_scraped'] = datetime.utcnow()

        item['last_post'] = datetime.utcfromtimestamp(0)

        comments = []

        if thread.get('ommited'):
            return item, comments

        comments.append(self.parse_comment(response, thread['op']))

        unparsed_comments = thread.get('posts')
        if unparsed_comments is None:
            return item, comments

        for comment in unparsed_comments:
            comments.append(self.parse_comment(response, comment))

        comment_timestamps = [comment['timestamp'] for comment in comments]
        item['last_post'] = max(comment_timestamps)

        return item, comments

    def parse_comment(self, response, comment):

        item = Comment()
        item['author'] = comment['poster_hash']
        item['text'] = comment['comment']

        item['timestamp'] = datetime.utcfromtimestamp(comment['timestamp'])

        item['id'] = comment['num']

        item['board'] = response.meta.get('board')

        item['thread'] = comment['thread_num']

        return item

    def open_mongodb(self):
        self.client = pymongo.MongoClient(
            host=settings.get('MONGO_HOST'),
            port=settings.get('MONGO_PORT'),
            username=settings.get('MONGO_USERNAME'),
            password=settings.get('MONGO_PASSWORD'),
            authSource=settings.get('MONGO_AUTHORIZATION_DATABASE'),
        )

        self.db = self.client[self.target_database]

    def close_mongodb(self):
        try:
            self.client.close()
        except AttributeError:
            pass

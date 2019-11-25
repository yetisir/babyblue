from scrapy import Spider
from crawl.items import Thread, Comment, Board
from datetime import datetime
from scrapy.conf import settings
import pymongo
import json
from json import JSONDecodeError


class RedditSpider(Spider):
    name = 'reddit'
    target_database = 'reddit'
    allowed_domains = ['reddit.com', 'pushshift.io']
    start_urls = [
        'http://www.reddit.com',
    ]

    boards_to_crawl = [
        'cryptocurrency',
    ]

    start_pushshift_board_threads_url = (
        'http://api.pushshift.io/reddit/search/submission'
        '/?subreddit={subreddit}&size=500'
        '&fields=title,author,id,subreddit_id,created_utc,full_link')
    pushshift_board_threads_url = (
        'http://api.pushshift.io/reddit/search/submission'
        '/?subreddit={subreddit}&size=500&before={before_timestamp}'
        '&fields=title,author,id,subreddit_id,created_utc,full_link')
    pushshift_thread_comments = (
        'http://api.pushshift.io/reddit/submission/comment_ids/{thread}')
    pushshift_comments_url = (
        'http://api.pushshift.io/reddit/comment/search?ids={comments}'
        '&fields=author,body,created_utc,id,subreddit_id')
    reddit_new_threads_url = (
        'http://www.reddit.com/r/{subreddit}/new.json?limit=100')
    reddit_new_comments_url = (
        'http://www.reddit.com/r/{subreddit}/comments.json?limit=100')
    reddit_aubreddit_about_url = (
        'http://www.reddit.com/r/{subreddit}/about')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.open_mongodb()

    def __del__(self):
        self.close_mongodb()

    def parse(self, response):
        if response.meta.get('type') is None:
            for board in self.boards_to_crawl:
                board_url = self.start_pushshift_board_threads_url.format(
                    subreddit=board)
                board_item = self.parse_board(board)
                yield response.follow(
                    board_url,
                    callback=self.parse,
                    meta={
                        'type': 'threads',
                        'board': board_item,
                        'add_board': True}
                )
        elif response.meta.get('type') == 'threads':

            board = response.meta.get('board')
            threads = json.loads(response.text)['data']
            min_timestamp = float('inf')
            for i, thread in enumerate(threads):
                item, timestamp = self.parse_thread(thread)
                comments_url = self.pushshift_thread_comments.format(
                    thread=item['id']
                )
                min_timestamp = min(timestamp, min_timestamp)
                if i == 0 and response.meta['add_board'] is True:
                    add_board = True
                yield response.follow(
                    comments_url,
                    callback=self.parse,
                    meta={
                        'board': response.meta['board'],
                        'thread': item,
                        'type': 'comment_ids',
                        'add_board': add_board,
                    }
                )

            if len(threads) != 0:
                board_url = self.pushshift_board_threads_url.format(
                    subreddit=board, before_timestamp=timestamp)
                board_item = self.parse_board(board)
                yield response.follow(
                    board_url,
                    callback=self.parse,
                    meta={
                        'type': 'threads',
                        'board': board_item,
                        'add_board': False}
                )

        elif response.meta.get('type') == 'comment_ids':

            comment_ids = json.loads(response.text)['data']

            for i in range(0, len(comment_ids), 500):
                comment_string = ','.join(comment_ids[i:i+500])
                comments_url = self.pushshift_comments_url.format(
                    comments=comment_string)

                yield response.follow(
                    comments_url,
                    callback=self.parse,
                    meta={
                        'board': response.meta['board'],
                        'thread': response.meta['thread'],
                        'type': 'comments',
                        'index': i,
                        'add_board': response.meta['add_board'],
                    }
                )

        elif response.meta.get('type') == 'comments':

            comment_timestamps = []
            comments = json.loads(response.text)['data']
            for comment in comments:
                item = self.parse_comment(comment, response.meta['thread']['id'])
                comments.append(item['timestamp'])
                yield item

            if response.meta.get('index') == 0:
                thread_item = response.meta.get('thread')
                thread_item['last_post'] = max(comment_timestamps)
                yield thread_item

                if response.meta['add_board'] is True:
                    board_item = response.meta('board')
                    board_item['last'] = thread_item['last_post']
                    board_item['id'] = thread_item['board']
                    yield board_item

    def parse_thread(self, thread):
        item = Thread()
        item['title'] = thread['title']

        item['id'] = thread['id']

        item['author'] = thread['author']

        item['board'] = thread['subreddit_id']

        item['url'] = thread['full_link']

        item['last_scraped'] = datetime.utcnow()

        item['last_post'] = datetime.utcfromtimestamp(0)

        return item, thread['created_utc']

    def parse_comment(self, comment, thread_id):

        item = Comment()
        import pdb; pdb.set_trace()
        item['author'] = comment['author']
        item['text'] = comment['body']
        item['timestamp'] = datetime.utcfromtimestamp(comment['created_utc'])
        item['id'] = comment['id']
        item['board'] = comment['subreddit_id']
        item['thread'] = thread_id

        return item

    def parse_board(self, board):

        item = Board()
        item['name'] = board
        item['id'] = None
        item['url'] = None
        item['description'] = board
        item['last_scraped'] = datetime.utcnow()
        item['last_post'] = datetime.utcfromtimestamp(0)

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

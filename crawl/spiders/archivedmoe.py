from scrapy import Spider
from scrapy.selector import Selector
from crawl.items import Board, Thread, Comment
from datetime import datetime
from scrapy.conf import settings
import pymongo
import json
from json import JSONDecodeError


class ArchivedMoeSpider(Spider):
    name = 'archivedmoe'
    target_database = 'fourchan'
    allowed_domains = ['archived.moe']
    start_urls = [
        'http://archived.moe',
    ]

    # https://archived.moe/_/api/chan/thread/?board=biz&num=904256
    # https://github.com/FoolCode/FoolFuuka-docs/blob/master/code_guide/documentation/api.rst

    boards_to_crawl = [
        'biz',
    ]

    board_api_url = '{base_url}/_/api/chan/index/?board={board}&page={page}'
    thread_api_url = '{base_url}/_/api/chan/thread/?board={board}&num={thread}'

    def __init__(self):
        self.open_mongodb()

    def __del__(self):
        self.close_mongodb()

    def parse_response(self, response):
        #TODO: better check
        try:
            return json.loads(response.text)
        except JSONDecodeError:
            return None

    def parse(self, response):
        response_data = self.parse_response(response)

        if response_data is None:
            for board in self.boards_to_crawl:
                board_url = self.board_api_url.format(
                    base_url=self.start_urls[0], board=board, page=1)
                print('yololloylyoylyololololololool')
                print(board_url)
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

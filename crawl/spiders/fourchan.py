from scrapy import Spider
from scrapy.selector import Selector
from crawl.items import Board, Thread, Comment
from datetime import datetime
from scrapy.conf import settings
import pymongo
import json


class FourChanSpider(Spider):
    name = 'fourchan'
    target_database = 'fourchan'
    allowed_domains = ['a.4cdn.org']
    start_urls = [
        'http://a.4cdn.org/boards.json',
    ]

    boards_to_crawl = [
        'biz',
    ]

    def __init__(self):
        self.open_mongodb()

    def __del__(self):
        self.close_mongodb()

    def normalize_response(self, response):

        response_data = json.loads(response.text)
        if type(response_data) == list:
            threads = []
            for page in response_data:
                threads += page['threads']

            return {'threads': threads}

        return response_data

    def parse(self, response):
        response_data = self.normalize_response(response)

        boards = self.check_list(response_data.get('boards'))
        for board in boards:
            item = self.parse_board(response, board)
            yield item

            if item['id'] in self.boards_to_crawl:

                yield response.follow(
                    item['url'],
                    callback=self.parse,
                    meta={'board': item['id']}
                )

        if response.meta.get('board') in self.boards_to_crawl:

            threads = self.check_list(response_data.get('threads'))
            for thread in threads:
                item = self.parse_thread(response, thread)
                yield item
                yield response.follow(
                    item['url'],
                    callback=self.parse,
                    meta={
                        'thread': item['id'],
                        'board': response.meta.get('board'),
                    }
                )
            comments = self.check_list(response_data.get('posts'))
            for comment in comments:
                item = self.parse_comment(response, comment)
                if item is not None:
                    yield item

    def check_list(self, list):
        if list is None:
            return []
        else:
            return list

    def parse_board(self, response, board):

        item = Board()
        item['name'] = board['title']

        item['id'] = board['board']

        item['url'] = ('http://a.4cdn.org/{board_id}/catalog.json').format(
                           board_id=item['id'])

        item['description'] = Selector(text=board['meta_description']).xpath(
            'normalize-space()').extract_first()

        item['last_scraped'] = datetime.utcnow()

        item['last_post'] = datetime.utcfromtimestamp(0)

        return item

    def parse_thread(self, response, thread):
        item = Thread()
        item['title'] = thread.get('sub')
        if item['title'] is None:
            item['title'] = ''

        item['id'] = thread['no']

        item['author'] = thread['id']

        item['board'] = response.meta.get('board')

        item['url'] = ('http://a.4cdn.org/'
                       '{board_id}/thread/{thread_id}.json').format(
                           board_id=item['board'], thread_id=item['id'])

        item['last_scraped'] = datetime.utcnow()

        item['last_post'] = datetime.utcfromtimestamp(thread['last_modified'])

        return item

    def parse_comment(self, response, comment):
        if comment.get('com') is None:
            return None
        item = Comment()
        item['author'] = comment['id']
        item['text'] = Selector(text=comment['com']).xpath(
            'normalize-space()').extract_first()

        item['timestamp'] = datetime.utcfromtimestamp(comment['time'])

        item['id'] = comment['no']

        item['board'] = response.meta.get('board')

        item['thread'] = response.meta.get('thread')

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

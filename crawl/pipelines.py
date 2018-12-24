# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo
#
# from scrapy.conf import settings
# from scrapy.exceptions import DropItem
from scrapy import log

from crawl.items import Board, Thread, Comment


class MongoPipeline(object):
    collections = {
        Board: 'boards',
        Thread: 'threads',
        Comment: 'comments',
    }

    def __init__(self, host, port, username, password, database):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            host=crawler.settings.get('MONGO_HOST'),
            port=crawler.settings.get('MONGO_PORT'),
            username=crawler.settings.get('MONGO_USERNAME'),
            password=crawler.settings.get('MONGO_PASSWORD'),
            database=crawler.settings.get('MONGO_DATABASE')
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            authSource=self.database,
        )

        self.db = self.client[self.database]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        doc = dict(item)
        spec = {'id': doc['id']}
        collection = self.collections.get(type(item))
        self.db[collection].update(spec, doc, True)
        return item

        # valid = True
        # for data in item:
        #     if not data:
        #         valid = False
        #         raise DropItem('Missing {0}'.format(data))
        # if valid:
        #     self.collection.insert(dict(item))
        #     log.msg('Question added to MongoDB database!')
        # return item

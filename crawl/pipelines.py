# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo
#
# from scrapy.conf import settings
# from scrapy.exceptions import DropItem
import logging
from crawl.items import Board, Thread, Comment

log = logging.getLogger('scrapy.proxies')


class MongoPipeline(object):
    collections = {
        Board: 'boards',
        Thread: 'threads',
        Comment: 'comments',
    }

    def __init__(self):
        pass

    @classmethod
    def from_crawler(cls, crawler):
        return cls()

    def open_spider(self, spider):
        pass

    def close_spider(self, spider):
        pass

    def process_item(self, item, spider):
        doc = dict(item)
        spec = {'id': doc['id']}
        collection = self.collections.get(type(item))
        spider.db[collection].update(spec, doc, True)
        return item

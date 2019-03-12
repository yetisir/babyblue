from scrapy import Spider
from scrapy.selector import Selector
from crawl.items import Board, Thread, Comment
from datetime import datetime
import dateparser
from scrapy.conf import settings
import pymongo


class BitcoinTalkSpider(Spider):
    name = "bitcointalk"
    allowed_domains = ["bitcointalk.org"]
    start_urls = [
        'https://bitcointalk.org/',
    ]

    boards_to_crawl = [
        'b1',
    ]

    def __init__(self):
        self.open_mongodb()

    def __del__(self):
        self.close_mongodb()

    def parse(self, response):
        boards = Selector(response).xpath(
            '//tr/td/b/a[starts-with(@name, "b")]/../../..'
        )

        threads = Selector(response).xpath(
            '//span[starts-with(@id, "msg")]/../..'
        )

        comments = Selector(response).xpath(
            '//div[starts-with(@id, "subject")]/../../../../../..'
        )

        next_page = Selector(response).xpath(
            '//span[@class="prevnext"]/*[text()[.="»"]]/@href'
        ).extract_first()

        for board in boards:

            item = self.parse_board(response, board)

            cached_board = self.db.boards.find_one({'id': item['id']})

            yield item

            scrape_board = False
            if not cached_board:
                scrape_board = True
            elif cached_board['last_scraped'] < item['last_post']:
                scrape_board = True

            if scrape_board:
                yield response.follow(
                    item['url'],
                    callback=self.parse,
                    meta={
                        'board': item['id'],
                    }
                )

        if response.meta.get('board') in self.boards_to_crawl:
            updated_threads = False
            for thread in threads:

                    item = self.parse_thread(response, thread)

                    cached_thread = self.db.threads.find_one(
                        {'id': item['id']}
                    )

                    yield item

                    scrape_thread = False
                    if not cached_thread:
                        scrape_thread = True
                    elif cached_thread['last_scraped'] < item['last_post']:
                        scrape_thread = True

                    if scrape_thread:

                        updated_threads = True

                        yield response.follow(
                            item['url'],
                            callback=self.parse,
                            meta={
                                'thread': item['id'],
                                'board': response.meta.get('board'),
                            }
                        )

            comment_thread = response.meta.get('thread')
            max_db_comment_id_document = self.db.comments.find_one(
                filter={'thread': comment_thread},
                sort=[('id', pymongo.DESCENDING)]
            )

            if max_db_comment_id_document is not None:
                max_db_comment_id = max_db_comment_id_document['id']
            else:
                max_db_comment_id = 0

            response_comment_ids = [0]
            print(comments)
            for comment in comments:
                item = self.parse_comment(response, comment)
                response_comment_ids.append(int(item['id']))
                yield item

            if next_page:
                print(response_comment_ids, max_db_comment_id)
                if (updated_threads or
                        min(response_comment_ids) > int(max_db_comment_id)):

                    yield response.follow(
                        next_page,
                        callback=self.parse,
                        meta=response.meta
                    )

    def parse_board(self, response, board):

        item = Board()
        item['name'] = board.xpath(
            'descendant::a[starts-with(@name, "b")]/text()'
        ).extract_first()

        item['id'] = board.xpath(
            'descendant::a[starts-with(@name, "b")]/@name'
        ).extract_first()

        item['url'] = board.xpath(
            'descendant::a[starts-with(@name, "b")]/@href'
        ).extract_first()

        item['description'] = ''.join(board.xpath(
            'descendant::td/text()'
        ).extract()).strip()

        item['last_scraped'] = datetime.utcnow()

        item['last_post'] = dateparser.parse(board.xpath(
            'descendant::b[contains(text(), "Last post")]/..'
        ).xpath('normalize-space()').extract_first().split('on ')[-1],
                                             settings={'TIMEZONE': 'UTC'})

        return self.verify_date(item, 'last_post')

    def parse_thread(self, response, thread):
        item = Thread()
        item['title'] = thread.xpath(
            'descendant::span[starts-with(@id, "msg")]/a/text()'
        ).extract_first()

        item['url'] = thread.xpath(
            'descendant::span[starts-with(@id, "msg")]/a/@href'
        ).extract_first()

        item['id'] = item['url'].split('topic=')[1].split('.')[0]

        item['author'] = thread.xpath(
            'descendant::a[starts-with(@title, "View")]/text()'
        ).extract_first()

        item['board'] = response.meta.get('board')

        item['last_scraped'] = datetime.utcnow()

        item['last_post'] = dateparser.parse(thread.xpath(
            'descendant::span[@class="smalltext"]'
        ).xpath('normalize-space()').extract_first().split(' by')[0],
                                             settings={'TIMEZONE': 'UTC'})

        return self.verify_date(item, 'last_post')

    def parse_comment(self, response, comment):
        item = Comment()
        item['author'] = comment.xpath(
            'descendant::td[@class="poster_info"]/b/a/text()'
        ).extract_first()

        item['text'] = comment.xpath(
            'descendant::div[@class="post"]'
        ).xpath('normalize-space()').extract_first()

        item['timestamp'] = dateparser.parse(comment.xpath(
            'descendant::table/descendant::div[@class="smalltext"]'
        ).xpath('normalize-space()').extract_first().split('Last')[0],
                                             settings={'TIMEZONE': 'UTC'})

        item['id'] = comment.xpath(
            'descendant::div[starts-with(@id, "subject")]/@id'
        ).extract_first().split('_')[-1]

        item['board'] = response.meta.get('board')

        item['thread'] = response.meta.get('thread')

        print(response.meta.get('thread'))
        print(comment.xpath(
            'descendant::table/descendant::div[@class="smalltext"]'
        ))
        print(comment.xpath(
            'descendant::table/descendant::div[@class="smalltext"]'
        ).xpath('normalize-space()'))
        print(comment.xpath(
            'descendant::table/descendant::div[@class="smalltext"]'
        ).xpath('normalize-space()').extract_first())
        return self.verify_date(item, 'timestamp')

    def verify_date(self, item, time_item):
        # Handle race condition - post may have been retrieved before
        # midnight but processed here shortly after midnight resulting in
        # the translation of 'Today' to datetime being off by one day

        if item[time_item] > datetime.utcnow():
            item[time_item] = item[time_item].shift(days=-1)

        return item

    def open_mongodb(self):

        self.client = pymongo.MongoClient(
            host=settings.get('MONGO_HOST'),
            port=settings.get('MONGO_PORT'),
            username=settings.get('MONGO_USERNAME'),
            password=settings.get('MONGO_PASSWORD'),
            authSource=settings.get('MONGO_DATABASE'),
        )
        print(self.client)

        self.db = self.client[settings.get('MONGO_DATABASE')]

    def close_mongodb(self):
        self.client.close()

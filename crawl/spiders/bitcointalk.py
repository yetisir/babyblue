from scrapy import Spider
from scrapy.selector import Selector
from crawl.items import Board, Thread, Comment
from datetime import datetime
import dateparser


class BitCoinTalkSpider(Spider):
    name = "bitcointalk"
    allowed_domains = ["bitcointalk.org"]
    start_urls = [
        'https://bitcointalk.org/',
    ]

    boards_to_crawl = [
        'b67',
    ]

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
            yield item

            yield response.follow(
                item['url'],
                callback=self.parse,
                meta={
                    'board': item['id'],
                }
            )
        print(response.meta.get('board'), self.boards_to_crawl)
        print(response.meta.get('board') in self.boards_to_crawl)
        if response.meta.get('board') in self.boards_to_crawl:
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

            for comment in comments:
                yield self.parse_comment(response, comment)

            if next_page:
                yield response.follow(next_page, callback=self.parse)

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
        ).xpath('normalize-space()').extract_first().split('on ')[-1])

        return self.verify_date(item, 'last_post')

    def parse_thread(self, response, thread):
        item = Thread()
        item['title'] = thread.xpath(
            'descendant::span[starts-with(@id, "msg")]/a/text()'
        ).extract_first()

        item['url'] = thread.xpath(
            'descendant::span[starts-with(@id, "msg")]/a/@href'
        ).extract_first()

        item['id'] = thread.xpath(
            'descendant::span[starts-with(@id, "msg")]/@id'
        ).extract_first().split('_')[-1]

        item['author'] = thread.xpath(
            'descendant::a[starts-with(@title, "View")]/text()'
        ).extract_first()

        item['board'] = response.meta.get('board')

        item['last_scraped'] = datetime.utcnow()

        item['last_post'] = dateparser.parse(thread.xpath(
            'descendant::span[@class="smalltext"]'
        ).xpath('normalize-space()').extract_first().split(' by')[0])

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
        ).xpath('normalize-space()').extract_first())

        item['id'] = comment.xpath(
            'descendant::div[starts-with(@id, "subject")]/@id'
        ).extract_first().split('_')[-1]

        item['board'] = response.meta.get('board')

        item['thread'] = response.meta.get('thread')

        return self.verify_date(item, 'timestamp')

    def verify_date(self, item, time_item):
        # Handle race condition - post may have been retrieved before
        # midnight but processed here shortly after midnight resulting in
        # the translation of 'Today' to be off by one day
        print(item, time_item)
        if item[time_item] > datetime.utcnow():
            item[time_item] = item[time_item].shift(days=-1)

        return item

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
f = requests.get()

https://api.pushshift.io/reddit/submission/comment_ids/6uey5x



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
    start_urls = [
        'http://www.reddit.com',
    ]

    boards_to_crawl = [
        'cryptocurrency',
    ]

    start_pushshift_board_threads_url = (
        'http://api.pushshift.io/reddit/search/submission'
        '/?subreddit={subreddit}&size=500'
        '&fields=title,author,id,subreddit_id')
    pushshift_board_threads_url = (
        'http://api.pushshift.io/reddit/search/submission'
        '/?subreddit={subreddit}&size=500&before={before_timestamp}'
        '&fields=title,author,id,subreddit_id')
    pushshift_thread_comments = (
        'http://api.pushshift.io/reddit/submission/comment_ids/{thread}')
    pushshift_comments_url = (
        'http://api.pushshift.io/reddit/comment/search?ids={comments}'
        '&fields=author,body,created_utc,id,subreddit_id')
    reddit_new_threads_url = (
        'http://www.reddit.com/r/{subreddit}/new.json?limit=100')
    reddit_new_comments_url = (
        'http://www.reddit.com/r/{subreddit}/comments.json?limit=100')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.open_mongodb()
        # if not hasattr(self, 'page'):
        #     self.page = 1
        # else:
        #     self.page = int(self.page)

    def __del__(self):
        self.close_mongodb()

    # def parse_response(self, response):
    #     # TODO: better check
    #     try:
    #         return json.loads(response.text)
    #     except JSONDecodeError:
    #         return None

    def parse(self, response):

        for board in self.boards_to_crawl:
            board_url = self.start_pushshift_board_threads_url.format(
                subreddit=board)
            yield response.follow(
                board_url,
                callback=self.parse_threads,
            )

    def parse_threads(self, response):
        board = response.meta.get('board')
        threads = json.loads(response.text)['data']
        for thread in threads:
            item = parse_thread(thread)
            comments_url = self.pushshift_thread_comments.format(
                thread=item['id']
            )
            yield response.follow(
                comments_url,
                callback=self.parse_comment_ids,
                meta={'thread': item[id]}

            )
        if len(threads) != 0:
            board_url = self.pushshift_board_threads_url.format(
                subreddit=board, before_timestamp=)
            yield response.follow(
                board_url,
                callback=self.parse_threads,
            )

    def parse_comment_ids(self, response):
        comment_ids = json.loads(response.text)['data']

        for i in range(0, len(comment_ids), 500):
            comment_string = ','.join(comment_ids[i:i+500])
            comments_url = self.pushshift_comments_url.format(
                comment_string)

            yield response.follow(
                comments_url,
                callback=self.parse_comments,
                meta={'thread': response.meta['thread']}
            )

    def parse_comments(self, response):
        comments = json.loads(response.text)['data']
        for comment in comments:
            item = self.parse_comment(response, comment)
            yield item

        # current_page = response.meta.get('page')
        # if current_page is not None and len(response_data) > 0:
        #     next_page = current_page + 1
        #     board_url = self.board_api_url.format(
        #         base_url=self.start_urls[0], board=board, page=next_page)

        #     yield response.follow(
        #         board_url,
        #         callback=self.parse,
        #         meta={'board': board, 'page': next_page}
        #     )

    def parse_thread(self, thread):
        item = Thread()
        item['title'] = thread['title']
        if item['title'] is None:
            item['title'] = ''

        item['id'] = thread['id']

        item['author'] = thread['author']

        item['board'] = thread['subreddit_id']

        # item['url'] = self.thread_api_url.format(
        #     base_url=self.start_urls[0], board=item['board'],
        #     thread=item['id'])

        item['last_scraped'] = datetime.utcnow()

        item['last_post'] = datetime.utcfromtimestamp(0)

        return item

        # comments = []

        # if thread.get('ommited'):
        #     return item, comments

        # comments.append(self.parse_comment(response, thread['op']))

        # unparsed_comments = thread.get('posts')
        # if unparsed_comments is None:
        #     return item, comments

        # for comment in unparsed_comments:
        #     comments.append(self.parse_comment(response, comment))

        # comment_timestamps = [comment['timestamp'] for comment in comments]
        # item['last_post'] = max(comment_timestamps)

        # return item, comments

    def parse_comment(self, response, comment):

        item = Comment()
        item['author'] = comment['author']
        item['text'] = comment['body']

        item['timestamp'] = datetime.utcfromtimestamp(comment['created_utc'])

        item['id'] = comment['id']

        item['board'] = comment['subreddit_id']

        item['thread'] = response.meta['thread']

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

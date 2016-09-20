import requests
import json
import math
import time
import pickle
from datetime import datetime
import functools
import sys
import config

posts_url = 'https://api.vk.com/method/wall.get?owner_id={}&\
           &access_token={}&v=5.52&filter=owner&count={}&offset={}'

posts_url_domain = 'https://api.vk.com/method/wall.get?domain={}&\
           &access_token={}&v=5.52&filter=owner&count={}&offset={}'


def get_posts(owner_id, count=5, offset=0):
    # owner_id - id of comunity
    url = posts_url.format(owner_id, config.TOKEN, count, offset)
    r = requests.get(url)
    return json.loads(r.text)


def outlier_condition(post):
    return post['likes']['count'] < config.MAX_LIKES and\
           post['likes']['count'] > config.MIN_LIKES and\
           len(post['text']) > config.MIN_CHARACTERS


def date_condition(min_date, post):
    return (datetime.now() - datetime.fromtimestamp(post['date'])).days >\
           config.MIN_DAYS_BEFORE_NOW and\
           (datetime.fromtimestamp(post['date']) - min_date).days >\
           config.MIN_DAYS_FROM_START


def text_post_condition(post):
    return post['post_type'] == 'post' and\
           len(post['text']) > config.MIN_CHARACTERS and\
           'http' not in post['text'] and\
           post.get('attachments') and\
           len([att for att in post['attachments']
                if att['type'] == 'video']) == 0


def parse_posts(posts):
    # filtering
    posts = filter(text_post_condition, posts)
    posts = filter(outlier_condition, posts)

    return [{'text': p['text'], 'likes': p['likes']['count'],
             'reposts':p['reposts']['count'],
             'date': p['date']} for p in posts]


def drop_duplicates(posts):
    unique_texts = set()
    unique_posts = []

    for post in posts[::-1]:
        if post['text'] in unique_texts:
            continue
        else:
            unique_posts.append(post)
            unique_texts.add(post['text'])
    return unique_posts


def download_posts(ownder_id, max_iter=None):
    result_posts = []
    post = get_posts(ownder_id, 1)
    count = int(post['response']['count'])
    print(count, 'total posts')
    iteration = math.ceil(count / 100)
    if max_iter:
        iteration = min(max_iter, iteration)

    for i in range(iteration):
        if i % 10 == 0:
            print('{:.2f}%'.format(i / iteration * 100))
        posts = get_posts(ownder_id, 100, i * 100)
        posts = parse_posts(posts['response']['items'])
        result_posts.extend(posts)
        time.sleep(0.3)

    # filterig by date
    min_date = datetime.fromtimestamp(min(result_posts,
                                      key=lambda x: x['date'])['date'])
    date_cond = functools.partial(date_condition, min_date)
    result_posts = list(filter(date_cond, result_posts))
    result_posts = drop_duplicates(result_posts)

    filename = 'data/{}.pkl'.format(ownder_id)
    with open(filename, 'wb') as f:
        pickle.dump(result_posts, f)
    print(len(result_posts), 'Posts added')
    return result_posts


def read_posts(owner_id):
    filename = 'data/{}.pkl'.format(owner_id)
    with open(filename, 'rb') as f:
        posts = pickle.load(f)
    return posts


if __name__ == '__main__':
    if len(sys.argv) > 1:
        owner_id = sys.argv[1]
        download_posts(owner_id)
    else:
        download_posts(config.OWNER_ID)

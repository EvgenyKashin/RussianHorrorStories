import requests
import json
import math
import time
import pickle
import numpy as np
import sklearn
import re
import matplotlib.pyplot as plt
from matplotlib.dates import YearLocator, MonthLocator, DateFormatter
from datetime import datetime
import functools
from stop_words import get_stop_words
import Stemmer
from sklearn.feature_extraction.text import CountVectorizer
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


def generate_dataset(posts):
    texts = [post['text'] for post in posts]
    likes = [post['likes'] for post in posts]
    return texts, likes


def clear_texts(texts):
    clear_texts = []
    stop_words = get_stop_words('ru')
    stemmer = Stemmer.Stemmer('russian')

    for text in texts:
        text = text.lower()
        text = re.sub('[^а-я]', ' ', text)
        text = text.split()
        text = [t for t in text if t not in stop_words]
        text = stemmer.stemWords(text)
        clear_texts.append(' '.join(text))
    return clear_texts


def plot_date_likes(posts):
    fig, ax = plt.subplots()

    years = YearLocator()   # every year
    months = MonthLocator()  # every month
    yearsFmt = DateFormatter('%Y')

    ax.xaxis.set_major_locator(years)
    ax.xaxis.set_major_formatter(yearsFmt)
    ax.xaxis.set_minor_locator(months)

    ax.set_xlabel('Year')
    ax.set_ylabel('Likes')
    ax.set_title('Horror stories')

    ax.plot_date([datetime.fromtimestamp(p['date']) for p in posts],
                 [p['likes'] for p in posts])

import requests
import json
import math
import time
import pickle
from datetime import datetime
import functools
import sys
import logging
import glob
from sklearn.preprocessing import StandardScaler
import config

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)
logging.getLogger('requests').setLevel(logging.ERROR)

posts_url = 'https://api.vk.com/method/wall.get?owner_id={}&\
           &access_token={}&v=5.52&filter=owner&count={}&offset={}'

posts_url_domain = 'https://api.vk.com/method/wall.get?domain={}&\
           &access_token={}&v=5.52&filter=owner&count={}&offset={}'


def get_posts(domain, count=5, offset=0):
    # domain - domain of comunity
    url = posts_url_domain.format(domain, config.TOKEN, count, offset)
    r = requests.get(url)
    return json.loads(r.text)


def outlier_condition(post):
    return post['likes']['count'] < config.MAX_LIKES and\
           post['likes']['count'] > config.MIN_LIKES and\
           len(post['text']) > config.MIN_CHARACTERS


def text_post_condition(post):
    return post['post_type'] == 'post' and\
           len(post['text']) > config.MIN_CHARACTERS and\
           'http' not in post['text'] and\
           (post.get('attachments') is None or
            len([att for att in post['attachments']
                 if att['type'] == 'video']) == 0)


def parse_posts(posts, domain):
    global not_texts_count, outliers_count

    # filtering
    before_filtering_count = len(posts)
    posts = list(filter(text_post_condition, posts))
    not_texts_count += before_filtering_count - len(posts)

    before_filtering_count = len(posts)
    posts = list(filter(outlier_condition, posts))
    outliers_count += before_filtering_count - len(posts)

    return [{'text': p['text'], 'likes': p['likes']['count'],
             'reposts':p['reposts']['count'], 'date': p['date'],
             'domain': domain} for p in posts]


def date_condition(min_date, post):
    return (datetime.now() - datetime.fromtimestamp(post['date'])).days >\
           config.MIN_DAYS_BEFORE_NOW and\
           (datetime.fromtimestamp(post['date']) - min_date).days >\
           config.MIN_DAYS_FROM_START


def filter_by_date(posts, min_date):
    global posts_count

    date_cond = functools.partial(date_condition, min_date)
    logger.debug('Filtering by date..')
    posts_filtered = list(filter(date_cond, posts))
    logger.debug('{:.2f}% dropped'.format(
                (len(posts) - len(posts_filtered)) / posts_count * 100))
    return posts_filtered


def drop_duplicates(posts, keep_first=True):
    global posts_count
    unique_texts = set()
    unique_posts = []

    logger.debug('Dropping duplicates posts..')
    if keep_first:
        posts = posts[::-1]
    for post in posts:
        if post['text'] in unique_texts:
            continue
        else:
            unique_posts.append(post)
            unique_texts.add(post['text'])

    if keep_first:
        posts = posts[::-1]
    logger.debug('{:.2f}% dropped'.format(
                (len(posts) - len(unique_posts)) / posts_count * 100))
    return unique_posts


not_texts_count = 0
outliers_count = 0
posts_count = 0


def download_posts(domain, max_iter=None, suffix='', save=True):
    global not_texts_count, outliers_count, posts_count
    not_texts_count, outliers_count, posts_count = 0, 0, 0
    start_time = time.time()

    logger.info('Downloading from {}'.format(domain))
    result_posts = []
    post = get_posts(domain, 1)
    posts_count = int(post['response']['count'])
    logger.info('{} total posts'.format(posts_count))
    iteration = math.ceil(posts_count / 100)
    if max_iter:
        iteration = min(max_iter, iteration)

    for i in range(iteration):
        if i % 20 == 0:
            logger.info('{:.2f}%'.format(i / iteration * 100))
        posts = get_posts(domain, 100, i * 100)
        posts = parse_posts(posts['response']['items'], domain + suffix)
        result_posts.extend(posts)
        time.sleep(0.25)

    # filterig by date
    min_date = datetime.fromtimestamp(min(result_posts,
                                      key=lambda x: x['date'])['date'])
    logger.debug('Min date: {}'.format(min_date))
    result_posts = filter_by_date(result_posts, min_date)
    result_posts = drop_duplicates(result_posts)

    logger.debug('Filtering: {:.2f}% outliers, {:.2f}% not text'.format(
                outliers_count / posts_count * 100,
                not_texts_count / posts_count * 100))

    if save:
        filename = 'data/{}.pkl'.format(domain + suffix)
        with open(filename, 'wb') as f:
            pickle.dump(result_posts, f)

    logger.info('{} posts from {} added. Domain: {}'.format(len(result_posts),
                posts_count, domain))
    logger.debug('Total time: {} sec'.format(round(time.time() - start_time)))

    return result_posts


def scale_posts_likes(posts, max_scaled_likes=7):
    likes = [[post['likes']] for post in posts]
    scaler = StandardScaler()
    likes = scaler.fit_transform(likes)

    logger.debug('Scaling likes..')
    for i in range(len(posts)):
        posts[i]['likes'] = likes[i]
    # filtering
    posts = [post for post in posts if post['likes'] <= 7]
    return posts


def add_labels(posts):
    sorted_posts = sorted(posts, key=lambda p: p['likes'])
    q1 = len(posts) // 4
    q3 = len(posts) // 4 * 3

    for i in range(len(posts)):
        if i < q1:
            sorted_posts[i]['label'] = 0
        elif i < q3:
            sorted_posts[i]['label'] = 1
        else:
            sorted_posts[i]['label'] = 2
    return sorted_posts


def drop_duplicates_and_scale(pattern='./data/*.pkl', scale=False,
                              labels=False):
    global posts_count
    result_posts = []

    for fn in glob.glob(pattern):
        if 'all_posts' in fn or 'dataset' in fn:
            continue
        posts = read_posts(filename=fn)
        if scale:
            posts = scale_posts_likes(posts)
        if labels:
            posts = add_labels(posts)
        result_posts.extend(posts)

    posts_count = len(result_posts)
    result_posts = drop_duplicates(result_posts)
    filename = 'data/dataset.pkl'
    with open(filename, 'wb') as f:
        pickle.dump(result_posts, f)
    logger.info('{} total posts added'.format(len(result_posts)))


def download_from_groups(domains):
    # with minimum constraints
    global posts_count

    result_posts = []
    for domain in domains:
        posts = download_posts(domain, save=False)
        result_posts.extend(posts)

    posts_count = len(result_posts)
    result_posts = drop_duplicates(result_posts, False)
    filename = 'data/all_posts.pkl'
    with open(filename, 'wb') as f:
        pickle.dump(result_posts, f)
    logger.info('{} total posts added'.format(len(result_posts)))


def read_posts(domain=None, filename=None):
    if not filename:
        if domain:
            filename = 'data/{}.pkl'.format(domain)
        else:
            raise Exception('Wrong arguments')
    with open(filename, 'rb') as f:
        posts = pickle.load(f)
    return posts


if __name__ == '__main__':
    if len(sys.argv) > 1:
        domain = sys.argv[1]
        download_posts(domain)
    else:
        # download_from_groups(config.DOMAINS)
        #  download_posts(config.DOMAINS[4], suffix='_2')
        # download_from_groups(config.DOMAINS)
        drop_duplicates_and_scale(scale=False)

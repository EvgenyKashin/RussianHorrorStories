import requests
import json
import math
import time
import pickle
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


def post_condition(post):
    return post['post_type'] == 'post' and\
           len(post['text']) > config.MIN_CHARACTERS and\
           'http:' not in post['text'] and\
           len([att for att in post['attachments']
                if att['type'] == 'video']) == 0


def parse_posts(posts):
    posts = list(filter(post_condition, posts['response']['items']))
    return [{'text': p['text'], 'likes': p['likes']['count'],
             'reposts':p['reposts']['count']} for p in posts]


def downloads_posts(ownder_id, max_iter=None):
    result_posts = []
    post = get_posts(ownder_id, 1)
    count = int(post['response']['count'])
    print(count, 'total posts')
    iteration = math.ceil(count / 100)
    filename = 'data/{}.txt'.format(ownder_id)
    if max_iter:
        iteration = min(max_iter, iteration)

    for i in range(iteration):
        if i % 10 == 0:
            print('{:.2f}%'.format(i / iteration * 100))
        posts = get_posts(ownder_id, 100, i * 100)
        posts = parse_posts(posts)
        result_posts.extend(posts)

        """
        with open(filename, 'a', encoding='utf-8') as f:
            f.write('\n\n'.join(posts))
            f.write('\n\n')
        """
        time.sleep(0.3)

    with open(filename, 'wb') as f:
        pickle.dump(result_posts, f)
    print(len(result_posts), 'Posts downloaded')
    return result_posts

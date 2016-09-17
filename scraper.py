import requests
import json
import config

posts_url = 'https://api.vk.com/method/wall.get?owner_id={}&\
           &access_token={}&v=5.52&filter=owner&count={}&offset={}'


def get_posts(owner_id, count=5, offset=0):
    # owner_id - id of comunity
    url = posts_url.format(owner_id, config.TOKEN, count, offset)
    r = requests.get(url)
    return json.loads(r.text)


def post_condition(post):
    return post['post_type'] == 'post' and\
           len(post['text']) > config.MIN_CHARACTERS


def parse_posts(posts):
    posts = list(filter(post_condition, posts['response']['items']))
    return posts

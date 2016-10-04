MIN_CHARACTERS = 600
MAX_LIKES = 10000000  # 4000
MIN_LIKES = 0  # 300
MIN_DAYS_BEFORE_NOW = -1   # 60
MIN_DAYS_FROM_START = -1  # 180

try:
    from private import *
except Exception as ex:
    print('Create private.py!')
    raise(ex)

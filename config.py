MIN_CHARACTERS = 600
MAX_LIKES = 4000
MIN_LIKES = 300
MIN_DAYS_BEFORE_NOW = 60
MIN_DAYS_FROM_START = 180

try:
    from private import *
except Exception as ex:
    print('Create private.py!')
    raise(ex)

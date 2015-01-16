from urlparse import urljoin
from time import sleep as delay
from functools import wraps

import requests
import pandas as pd
from bs4 import BeautifulSoup


time_between_requests = .5
def throttled(f):
    ''' A function for throttling http requests '''
    @wraps(f)
    def wrapper(*args, **kwargs):
        print 'Throttling request: {}:'.format(*args)
        delay(time_between_requests)
        return f(*args, **kwargs)
    return wrapper

baseurl = r'http://www.basketball-reference.com/'
def relativeurl_to_absolute(url):
    return urljoin(baseurl, url)

def consolidate_dfs(dfs):
    return pd.concat(dfs, axis=0)

def get_backtobacks(iterable):
    ''' Returns True if a team has played a game in the previous 36 hours '''
    l = pd.to_datetime(iterable)
    
    # 36 hours in seconds 
    thirtysixhours = 129600

    return [i > 0 and (gamedate - l[i-1]).total_seconds() <= thirtysixhours
            for i, gamedate in enumerate(l)]

@throttled
def search_player(playername):
    searchurl = "http://www.basketball-reference.com/search/search.fcgi"
    pg = requests.get(searchurl, params={'search': playername})
    pg.raise_for_status()
    soup = BeautifulSoup(pg.text)
    if soup.find(id='totals'):
        return pg.text
    else:
        raise ValueError('No player or non-unique player: {}'.format(playername))
        soup = BeautifulSoup(pg.text)

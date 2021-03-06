import cPickle as pickle
import gzip
import re

from urlparse import urljoin
from time import sleep as delay
from functools import wraps

import requests
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup

def load(filename):
    with gzip.open(filename) as f:
        return pickle.load(f)

searchurl = "http://www.basketball-reference.com/search/search.fcgi"
time_between_requests = .5

def throttled(f):
    ''' A function for throttling http requests '''
    @wraps(f)
    def wrapper(*args, **kwargs):
        print 'Throttling request: {}:'.format(args)
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
    ''' Returns the player data URL '''
    pg = requests.get(searchurl, params={'search': playername})
    pg.raise_for_status()

    if 'player' in pg.url:
        # We've been redirected to the player page because it was the only 
        # result in professional basketball
        return pg.url
    soup = BeautifulSoup(pg.text)
    
    search_results = soup.find_all(class_='search-item')
    for result in search_results:
        for tag in result.find_all(lambda x: x.name=='a' and
                                   'players' in x.get('href') 
                                   and 'nbdl' not in x.get('href')
                                   and 'euro' not in x.get('href')):
            if tag.string.lower().startswith(playername.lower()):
                return relativeurl_to_absolute(tag.get('href'))
    else:
        raise ValueError('No player or non-unique player: {}'.format(playername))
        soup = BeautifulSoup(pg.text)

@throttled 
def search_team(teamname):
    pg = requests.get(searchurl, params={'search': teamname})
    pg.raise_for_status()

    soup = BeautifulSoup(pg.text)
    def is_team_url(l):
        if l.name != 'a':
            return False
        url = l.get('href')
        return bool(re.match(r'^http://.*/teams/.*/', url))
    return relativeurl_to_absolute(soup.find(name='a', href=re.compile(r'^/teams/.*/')).get('href'))

def streak(iterable, predicate):
    iterable = list(int(x) for x in iterable)
    strk = np.zeros(len(iterable), dtype=np.uint8)

    prev = False
    for i, x in enumerate(iterable):
        v = 1 if predicate(x) else 0 
        if v and i > 0:
            v += strk[i-1]
        strk[i] = v

    return strk

class ParseError(Exception):
    pass

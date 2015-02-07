import datetime
import re

import requests
import pandas as pd
from bs4 import BeautifulSoup

from common import throttled, relativeurl_to_absolute, get_backtobacks
from common import consolidate_dfs, search_team
from common import ParseError

minimum_update_interval = datetime.timedelta(hours=10)

class Team(object):
    def __init__(self, name=None, abbrev=None):
        if abbrev:
            self.abbrev = abbrev.upper()
            self.name = None
            self.url = 'http://basketball-reference.com/teams/{}/'.format(self.abbrev)
        elif name:
            self.name = name
            self.url = search_team(name)
            self.abbrev = self.url[-4:-1]
        else:
            raise ValueError('No name or abbreviation!')
        self.gamelog = None
        self.last_updated = datetime.datetime.min

    def __repr__(self):
        return 'Team: {}'.format(self.abbrev)

    def update(self, force=False):
        if (not force) and (datetime.datetime.now() - self.last_updated) < minimum_update_interval:
            return
        elif self.gamelog is not None:
            curseason = self.gamelog.Season.max()
            prevdata = self.gamelog[self.gamelog.Season < curseason]
            newdata = self._get_gamelog(after=curseason)
            self.gamelog = consolidate_dfs([prevdata, newdata])
        else:
            self.gamelog = self._get_gamelog()
        self.last_updated = datetime.datetime.now()

    @throttled
    def _links_to_gamelogs(self, after=None):
        pg = requests.get(self.url)
        pg.raise_for_status()

        soup = BeautifulSoup(pg.text)
        seasonlinks = soup.find_all(name='a',
                                    href=re.compile(r'^/teams/[a-zA-Z]{3}/\d*\.html$'),
                                    text=re.compile(r'\d{4}-\d{2}'))
        seasonlinks = [x for x in seasonlinks if x.string >= after]
        seasonlinks = sorted(set(seasonlinks))
        seasonlinks = [x.get('href').replace('.html','/gamelog/') for x in seasonlinks]
        seasonlinks = [relativeurl_to_absolute(x) for x in seasonlinks]
        return seasonlinks

    def _get_gamelog(self, after=None):
        seasons = consolidate_dfs([self._get_season_gamelog(x) for x in
                                   self._links_to_gamelogs(after=after)])
        return seasons

    @throttled
    def _get_season_gamelog(self, url):
        pg = requests.get(url)
        pg.raise_for_status()
        
        soup = BeautifulSoup(pg.text)
        basic = self.process_gamelog(soup.find(id='tgl_basic'))
        advanced = self.process_gamelog(soup.find(id='tgl_advanced'))
        stats = self.merge_basic_and_advanced(basic, advanced)
        stats['Playoff'] = 0

        if soup.find(id='tgl_basic_playoffs'):
            pbasic = self.process_gamelog(soup.find(id='tgl_basic_playoffs'))
            padvanced = self.process_gamelog(soup.find(id='tgl_advanced_playoffs'))
            pstats = self.merge_basic_and_advanced(pbasic, padvanced)
            pstats['Playoff'] = 1
            stats = pd.concat([stats, pstats], axis=0)

        season = soup.find(lambda x: x.name == 'h1' and 'Team Game Log' in x.string)
        season = season.string.split()[0]
        stats['Season'] = season
        
        stats.drop([x for x in stats.columns if x.startswith('Unnamed') or x == 'W/L'], 
                   inplace=True, axis=1)
        stats.set_index('Date', inplace=True, drop=False)
        return stats
    
    def merge_basic_and_advanced(self,basic, advanced):
        advcols = ['Date'] + [x for x in advanced.columns if x not in basic.columns]
        a = advanced[advcols]
        return pd.merge(basic, a, on='Date')

    def process_gamelog(self, tags):

        def get_OTs(series):
            for x in series:
                if len(x) == 1: return 0
                y = x.encode('ascii', errors='replace').replace('?', ' ').translate(None, '()')
                y = y.split()
                
                return int(y[1])
                                
        for tag in tags.find_all(class_='thead'):
            tag.decompose()
        for tag in tags.find_all(class_='over_header'):
            tag.decompose()

        stats = pd.io.html.read_html(str(tags))[0]
        
        # Fix column names
        nc = [str(x) for x in stats.columns.values]
        nc[3] = 'Away'
        nc[6] = 'TmPTS'
        nc[7] = 'OppPTS'
        nc = [c if not c.endswith('.1') else 'Opp{}'.format(c[:-2]) for c in nc]
        stats.columns = nc

        stats['Date'] = pd.to_datetime(stats['Date'], coerce=True)
        stats['Away'] = stats['Away'] == '@'
        stats['Margin'] = stats.TmPTS - stats.OppPTS
        stats['Win'] = stats.Margin > 0 
        stats['OT'] = get_OTs(stats['W/L'])

        if 'TS%' not in stats.columns:
            # Stuff to do to basic stats
            stats['2P'] = stats['FG'] - stats['3P']
            stats['2PA'] = stats['FGA'] - stats['3PA']
            stats['2P%'] = stats['2P'] / stats['2PA']
            stats['Opp2P'] = stats['OppFG'] - stats['Opp3P']
            stats['Opp2PA'] = stats['OppFGA'] - stats['Opp3PA']
            stats['Opp2P%'] = stats['Opp2P'] / stats['Opp2PA']
        
        return stats
                                

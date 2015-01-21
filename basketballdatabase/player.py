from itertools import ifilter
from datetime import timedelta, datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup

from common import throttled, relativeurl_to_absolute, get_backtobacks
from common import consolidate_dfs, search_player
from common import ParseError

#### 
minimum_update_interval = timedelta(hours=10)

class Player(object):
    def __init__(self, name):
        self.name = name
        self._gamelog = None
        self.last_updated = datetime.min
        self._player_url = search_player(self.name)
        
    def __repr__(self):
        return 'Player: {}'.format(self.name)

    @property
    def gamelog(self):
        if self._gamelog is None:
            self._gamelog = self.__get_gamelog()
        return self._gamelog

    def update(self):
        if (datetime.now() - self.last_updated) >= minimum_update_interval:
            self._gamelog = self.__get_gamelog()


    def __links_to_player_season_gamelogs(self, page, after=None):
        pg = requests.get(self._player_url)
        pg.raise_for_status()

        soup = BeautifulSoup(pg.text)
        
        links = (link for link in soup.find_all('a')
                 if 'gamelog' in link.get('href'))
        if after:
            links = ifilter(lambda x: x.text >= after)

        links = sorted(set(links))
        for link in links:
            yield relativeurl_to_absolute(link.get('href'))


    def __get_gamelog(self):
        
        playerpage = self._player_url

        seasons = consolidate_dfs(
            self.get_player_season_data_from_url(url)
            for url in self.__links_to_player_season_gamelogs(playerpage))
        self.last_updated = datetime.now()
        return seasons

    @throttled
    def get_player_season_data_from_url(self, url):
        pg = requests.get(url)
        soup = BeautifulSoup(pg.text)
    
        basicstats = self.process_gamelog(soup.find(id='pgl_basic'))
        advanced = self.process_gamelog(soup.find(id='pgl_advanced'))
        stats = self.merge_basic_and_advanced(basicstats, advanced)

        stats['Playoff'] = False

        # Find the season
        season = soup.find(lambda x: x.name =='h1' and 'Game Log' in x.string)
        if not season:
            raise ParseError("Couldn't determine season!")
        season = season.string.split()[-3]
        stats['Season'] = season
        
        if not soup.find(id='pgl_basic_playoffs'):
            return stats
        else:
            playoffbasic = self.process_gamelog(soup.find(id='pgl_basic_playoffs'))
            playoffadv = self.process_gamelog(soup.find(id='pgl_advanced_playoffs'))
            playoffstats = self.merge_basic_and_advanced(playoffbasic, playoffadv)
            playoffstats['Playoff'] = True
            playoffstats['Season'] = season
            stats = pd.concat([stats, playoffstats], axis=0)

        return stats

    def merge_basic_and_advanced(self,basic, advanced):
        advcols = ['Date'] + [x for x in advanced.columns if x not in basic.columns]
        a = advanced[advcols]
        return pd.merge(basic, a, on='Date')


    def process_gamelog(self, tag):
        # Functions for cleaning the data
        def delete_multiheader(table):
            '''
            Delete the extra header that shows up every 20 rows
            or so in basketball-reference.com data
            '''
            for row in table.find_all('tr', class_='thead'):
                row.decompose()
            return table

        def duration_fixer(duration):
            try:
                mins, secs = [int(x) for x in duration.split(':')]
                return timedelta(minutes=mins, seconds=secs)
            except:
                return 0

        def margin_parser(margin):
            winloss, margin = margin.split()
            margin = str(margin).translate(None, '()+')
            return int(margin)

        def age_parser(age):
            y, d = age.split('-')
            return int(y) + float(d) / 365.242
 
        tag = delete_multiheader(tag)
        # There should only be one table in the tag
        df = pd.io.html.read_html(str(tag))[0]
    
        # Minutes played gets interpreted as a string 
        df.MP = pd.to_timedelta([duration_fixer(x) for x in df.MP])
        
        # Decimalized minutes played
        df['MPd'] = [x.total_seconds() / 60 for x in df.MP]

        # Give names to the unnamed columns
        df.rename(columns = { u'Unnamed: 5': 'Away',  u'Unnamed: 7': 'Margin' }, 
                  inplace=True)

        # The 'Away' column has an '@' if it is away, else nothing'
        df.Away = df.Away == '@'
    
        # Get the win/loss margin in numeric format
        df.Margin = [margin_parser(margin) for margin in df.Margin]
    
        # Age is given as Years-Days, I'll convert to decimal
        df.Age = [age_parser(age) for age in df.Age]

        # Get 2P statistics for basic stats
        # True shooting percentage (TS%) is not in the basic stats 
        if 'TS%' not in df.columns:
            df['2P'] = df['FG'] - df['3P']
            df['2PA'] = df['FGA'] - df['3PA']
            df['2P%'] = df['2P'] / df['2PA']

        # A column for when a player is on a back-to-back
        df['back2back'] = get_backtobacks(df.Date)

        # Set the index to the date
        df = df.set_index('Date', drop=False)
        return df

if __name__ == '__main__':
    t = Player('Lebron James')
    from IPython import embed
    embed()
    print 'Done'

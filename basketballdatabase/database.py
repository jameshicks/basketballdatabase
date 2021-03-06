import cPickle as pickle
import gzip

import pandas as pd

from player import Player
from team import Team

class Database(object):
    def __init__(self):
        self.players = dict()
        self.teams = dict()

    def nplayers(self):
        return len(self.players)

    def nteams(self):
        return len(self.teams)

    def apply_statistic(self, name, func):
        for p in self.players.values():
            p.apply_statistic(name, func)

    def search_player(self, name):
        if name not in self.players:
            p = Player(name)
            self.players[p.name] = p
        return self.players[name]

    def search_team(self, abbrev):
        if abbrev not in self.teams:
            t = Team(abbrev=abbrev)
            self.teams[t.abbrev] = t
        return self.teams[abbrev]

    def remove_player(self, name):
        del self.players[name]

    def save(self, filename):
        with gzip.open(filename, 'w', compresslevel=6) as f:
            pickle.dump(self, f, pickle.HIGHEST_PROTOCOL)

    def update_players(self):
        for player in self.players.values():
            player.update()
    
    def update_teams(self):
        for team in self.teams.values():
            team.update()

    def update(self):
        self.update_players()
        self.update_teams()

    @property
    def observed_teams(self):
        def obteams(player):
            if player.gamelog is None:
                return set()
            return set.union(set(player.gamelog.Tm), set(player.gamelog.Opp))
        teams = reduce(set.union, (obteams(x) for x in self.players.values()))
        teams = set.union(teams, set(self.teams.keys()))
        return teams

    @property
    def player_gamelogs(self):
        return pd.concat([x.gamelog for x in self.players.values()])

    @property
    def team_gamelogs(self):
        return pd.concat([x.gamelog for x in self.teams.values()])

if __name__ == '__main__':
    import IPython; IPython.embed()

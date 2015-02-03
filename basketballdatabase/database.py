import cPickle as pickle
import gzip

import pandas as pd

from player import Player

class Database(object):
    def __init__(self):
        self.players = dict()

    def nplayers(self):
        return len(self.players)

    def apply_statistic(self, name, func):
        for p in self.players.values():
            p.apply_statistic(name, func)

    def search_player(self, name):
        if name not in self.players:
            p = Player(name)
            self.players[p.name] = p
        return p

    def remove_player(self, name):
        del self.players[name]

    def save(self, filename):
        with gzip.open(filename, 'w') as f:
            pickle.dump(self, f, pickle.HIGHEST_PROTOCOL)

    def update(self):
        for player in self.players.values():
            player.update()

    @property
    def player_gamelogs(self):
        return pd.concat([x.gamelog for x in self.players.values()])

if __name__ == '__main__':
    import IPython; IPython.embed()

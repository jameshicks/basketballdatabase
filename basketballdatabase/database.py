import cPickle as pickle
import gzip

from player import Player

class Database(object):
    def __init__(self):
        self.players = dict()

    def search_player(self, name):
        if name not in self.players:
            p = Player(name)
            self.players[p.name] = p
        return p

    def save(self, filename):
        with gzip.open(filename, 'w') as f:
            pickle.dump(self, f, pickle.HIGHEST_PROTOCOL)

    def update(self):
        for player in self.players.values():
            player.update()

if __name__ == '__main__':
    import IPython; IPython.embed()

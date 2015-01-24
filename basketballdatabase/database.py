import cPickle as pickle
import gzip

from player import Player

class Database(object):
    def __init__(self):
        self.players = set()

    def search_player(self, name):
        p = Player(name)
        self.players.add(p)
        return p

    def save(self, filename):
        with gzip.open(filename, 'w') as f:
            pickle.dump(self, f, pickle.HIGHEST_PROTOCOL)

if __name__ == '__main__':
    import IPython; IPython.embed()

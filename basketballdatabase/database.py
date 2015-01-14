from player import Player

class Database(object):
    def __init__(self):
        self.players = set()

    def search_player(self, name):
        p = Player(name)
        self.players.add(p)
        return p

if __name__ == '__main__':
    import IPython; IPython.embed()

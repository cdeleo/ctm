import collections

Event = collections.namedtuple('Event', ['name'])
Player = collections.namedtuple('Player', ['id', 'name'])
Scan = collections.namedtuple('Scan', ['id', 'player', 'data'])

class AlreadyExistsError(Exception):
  pass

class NotFoundError(Exception):
  pass

class UnavailableError(Exception):
  pass

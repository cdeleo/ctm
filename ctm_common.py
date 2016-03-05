import collections

Event = collections.namedtuple('Event', ['name'])
Player = collections.namedtuple('Player', ['id', 'name', 'scan_id'])
Scan = collections.namedtuple('Scan', ['id', 'player_id', 'data'])

class AlreadyExistsError(Exception):
  pass

class NotFoundError(Exception):
  pass

class UnavailableError(Exception):
  pass

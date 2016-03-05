import ctm_common

import base64
import contextlib
import msgpack
import os
import os.path
import random
import shutil
import struct
import subprocess
import zerorpc

EVENT_PREFIX = 'e'
LOCK_PREFIX = 'l'
MASTER_PREFIX = 'm'
PLAYER_PREFIX = 'p'
SCAN_PREFIX = 's'
SCAN_DATA_PREFIX = 'd'

LOCK_TIMEOUT_SECONDS = 5


@contextlib.contextmanager
def Lock(path, shared):
  f = open(path, 'a')
  r = subprocess.call(
      ['flock',
       '-s' if shared else '-x',
       '-w %d' % LOCK_TIMEOUT_SECONDS,
       str(f.fileno())])
  if r != 0:
    raise ctm_common.UnavailableError()
  try:
    yield
  finally:
    r = subprocess.call(
      ['flock',
       '-u',
       str(f.fileno())])
    f.close()


class CtmServer(object):

  def __init__(self, working_dir):
    self._working_dir = working_dir

  def _IsValidPath(self, prefix, name, event_name):
    if event_name:
      return True
    elif prefix == EVENT_PREFIX:
      return True
    elif prefix == MASTER_PREFIX and name == EVENT_PREFIX:
      return True
    return False

  def _GetPath(self, prefix, name, event_name=None, lock=False):
    if not self._IsValidPath(prefix, name, event_name):
      raise Exception('Invalid call: _GetPath("%s", "%s", "%s", %s)' %
                      (prefix, name, event_name, str(lock)))
    parts = [self._working_dir]
    if event_name:
      parts.append(EVENT_PREFIX + event_name)
    if lock:
      parts.append(LOCK_PREFIX + prefix + str(name))
    else:
      parts.append(prefix + str(name))
    return os.path.join(*parts)

  def _List(self, prefix, event_name=None):
    if event_name:
      path = self._GetPath(EVENT_PREFIX, event_name)
    else:
      path = self._working_dir
    return [r[1:] for r in os.listdir(path)
            if r[0] == prefix]

  def _Lock(self, shared, *args, **kwargs):
    kwargs['lock'] = True
    path = self._GetPath(*args, **kwargs)
    return Lock(path, shared)

  def _ExclusiveLock(self, *args, **kwargs):
    return self._Lock(False, *args, **kwargs)

  def _SharedLock(self, *args, **kwargs):
    return self._Lock(True, *args, **kwargs)

  def _ReadPlayer(self, event_name, player_id):
    with open(self._GetPath(PLAYER_PREFIX, player_id,
                            event_name=event_name), 'r') as player_file:
      return ctm_common.Player(*msgpack.unpack(player_file))

  def _WritePlayer(self, event_name, player):
    with open(self._GetPath(PLAYER_PREFIX, player.id,
                            event_name=event_name), 'w') as player_file:
      msgpack.pack(player, player_file)

  def _ModifyPlayerScanId(self, event_name, player_id, scan_id):
    player = self._ReadPlayer(event_name, player_id)
    player = ctm_common.Player(player.id, player.name, scan_id)
    self._WritePlayer(event_name, player)

  def _GetScanId(self, event_name):
    while True:
      scan_id = base64.b32encode(struct.pack('Q', random.getrandbits(64)))[:-3]
      if not os.path.exists(
          self._GetPath(SCAN_PREFIX, scan_id, event_name=event_name)):
        return scan_id

  def _ReadScan(self, event_name, scan_id):
    with open(self._GetPath(SCAN_PREFIX, scan_id,
                            event_name=event_name), 'r') as scan_file:
      return ctm_common.Scan(*msgpack.unpack(scan_file))

  def _ReadScanData(self, event_name, scan_id):
    with open(self._GetPath(SCAN_DATA_PREFIX, scan_id,
                            event_name=event_name), 'r') as scan_data_file:
      return scan_data_file.read()

  def _WriteScan(self, event_name, scan):
    with open(self._GetPath(SCAN_PREFIX, scan.id,
                            event_name=event_name), 'w') as scan_file:
      msgpack.pack(scan, scan_file)

  def _WriteScanData(self, event_name, scan_id, data):
    with open(self._GetPath(SCAN_DATA_PREFIX, scan_id,
                            event_name=event_name), 'w') as scan_data_file:
      scan_data_file.write(data)

  @contextlib.contextmanager
  def _EnsureEvent(self, event_name):
    with self._SharedLock(EVENT_PREFIX, event_name):
      if not os.path.isdir(self._GetPath(EVENT_PREFIX, event_name)):
        raise ctm_common.NotFoundError('Event %s does not exist.' % event_name)
      yield

  @contextlib.contextmanager
  def _Empty(self):
    yield

  # Event management
  def ListEvents(self):
    with self._SharedLock(MASTER_PREFIX, EVENT_PREFIX):
      results = []
      for event_name in self._List(EVENT_PREFIX):
        if os.path.isdir(self._GetPath(EVENT_PREFIX, event_name)):
          results.append(ctm_common.Event(event_name))
      return results

  def CreateEvent(self, event_name):
    with self._ExclusiveLock(MASTER_PREFIX, EVENT_PREFIX):
      with self._ExclusiveLock(EVENT_PREFIX, event_name):
        try:
          os.mkdir(self._GetPath(EVENT_PREFIX, event_name))
        except OSError:
          raise ctm_common.AlreadyExistsError(
              'Event %s already exists.' % event_name)

  def DeleteEvent(self, event_name):
    with self._ExclusiveLock(MASTER_PREFIX, EVENT_PREFIX):
      with self._ExclusiveLock(EVENT_PREFIX, event_name):
        try:
          shutil.rmtree(self._GetPath(EVENT_PREFIX, event_name))
        except OSError:
          raise ctm_common.NotFoundError(
              'Event %s does not exist.' % event_name)

  # Player management
  def ListPlayers(self, event_name):
    results = []
    with self._EnsureEvent(event_name):
      with self._SharedLock(
          MASTER_PREFIX, PLAYER_PREFIX, event_name=event_name):
        for player_id in self._List(PLAYER_PREFIX, event_name=event_name):
          with self._SharedLock(
              PLAYER_PREFIX, player_id, event_name=event_name):
            results.append(self._ReadPlayer(event_name, player_id))
    return results

  def SetPlayers(self, event_name, players):
    with self._EnsureEvent(event_name):
      with self._ExclusiveLock(
          MASTER_PREFIX, PLAYER_PREFIX, event_name=event_name):
        for player_id in self._List(PLAYER_PREFIX, event_name=event_name):
          with self._ExclusiveLock(
              PLAYER_PREFIX, player_id, event_name=event_name):
            os.remove(
                self._GetPath(PLAYER_PREFIX, player_id, event_name=event_name))
        for player in players:
          with self._ExclusiveLock(
              PLAYER_PREFIX, player.id, event_name=event_name):
            self._WritePlayer(event_name, player)

  # Scan management
  def ListScans(self, event_name, unmarked_only=False):
    results = []
    with self._EnsureEvent(event_name):
      with self._SharedLock(
          MASTER_PREFIX, SCAN_PREFIX, event_name=event_name):
        for scan_id in self._List(SCAN_PREFIX, event_name=event_name):
          with self._SharedLock(SCAN_PREFIX, scan_id, event_name=event_name):
            scan = self._ReadScan(event_name, scan_id)
            if not unmarked_only or scan.player_id is None:
              results.append(scan)
    return results

  def GetScan(self, event_name, scan_id):
    with self._EnsureEvent(event_name):
      with self._SharedLock(
          MASTER_PREFIX, SCAN_PREFIX, event_name=event_name):
        if not os.path.exists(
            self._GetPath(SCAN_PREFIX, scan_id, event_name=event_name)):
          raise ctm_common.NotFoundError('Scan %s does not exist.' % scan_id)
        with self._SharedLock(SCAN_PREFIX, scan_id, event_name=event_name):
          scan = self._ReadScan(event_name, scan_id)
          if os.path.exists(
              self._GetPath(SCAN_DATA_PREFIX, scan_id, event_name=event_name)):
            data = self._ReadScanData(event_name, scan_id)
            scan = ctm_common.Scan(scan.id, scan.player_id, data)
          return scan

  def PostScan(self, event_name, data):
    with self._EnsureEvent(event_name):
      with self._ExclusiveLock(
          MASTER_PREFIX, SCAN_PREFIX, event_name=event_name):
        scan_id = self._GetScanId(event_name)
        scan = ctm_common.Scan(scan_id, None, None)
        with self._ExclusiveLock(SCAN_PREFIX, scan_id, event_name=event_name):
          self._WriteScan(event_name, scan)
          self._WriteScanData(event_name, scan_id, data)
          return scan_id

  def MarkScan(self, event_name, scan_id, player_id=None):
    with self._EnsureEvent(event_name):
      with self._SharedLock(
          MASTER_PREFIX, SCAN_PREFIX, event_name=event_name):
        if not os.path.exists(
            self._GetPath(SCAN_PREFIX, scan_id, event_name=event_name)):
          raise ctm_common.NotFoundError('Scan %s does not exist.' % scan_id)
        with self._ExclusiveLock(SCAN_PREFIX, scan_id, event_name=event_name):
          if player_id is not None:
            new_player_lock = self._ExclusiveLock(
                PLAYER_PREFIX, player_id, event_name=event_name)
          else:
            new_player_lock = self._Empty()
          with new_player_lock:
            if (player_id is not None and
                not os.path.exists(
                    self._GetPath(PLAYER_PREFIX, player_id,
                                  event_name=event_name))):
              raise ctm_common.NotFoundError(
                  'Player %s does not exist.' % player_id)

            existing_scan = self._ReadScan(event_name, scan_id)
            if player_id == existing_scan.player_id:
              return
            new_scan = ctm_common.Scan(scan_id, player_id, None)
            self._WriteScan(event_name, new_scan)

            if existing_scan.player_id is not None:
              old_player_lock = self._ExclusiveLock(
                  PLAYER_PREFIX, existing_scan.player_id,
                  event_name=event_name)
            else:
              old_player_lock = self._Empty()
            with old_player_lock:
              if existing_scan.player_id is not None:
                self._ModifyPlayerScanId(
                    event_name, existing_scan.player_id, None)
              if player_id is not None:
                self._ModifyPlayerScanId(event_name, player_id, scan_id)

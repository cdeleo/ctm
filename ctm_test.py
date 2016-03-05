import ctm_common
import ctm_server

import shutil
import tempfile
import unittest


class TestCtm(unittest.TestCase):

  def setUp(self):
    self.working_dir = tempfile.mkdtemp()
    self.server = ctm_server.CtmServer(self.working_dir)

  def tearDown(self):
    shutil.rmtree(self.working_dir)

  def testListEventsEmpty(self):
    expected = []
    actual = self.server.ListEvents()
    self.assertEqual(actual, expected)

  def testCreateEvent(self):
    self.server.CreateEvent('test')

    expected = [ctm_common.Event('test')]
    actual = self.server.ListEvents()
    self.assertEqual(actual, expected)

  def testCreateEventDuplicate(self):
    self.server.CreateEvent('test')
    self.assertRaises(
        ctm_common.AlreadyExistsError,
        self.server.CreateEvent, 'test')

  def testDeleteEventError(self):
    self.assertRaises(
        ctm_common.NotFoundError,
        self.server.DeleteEvent, 'test')

  def testDeleteEvent(self):
    self.server.CreateEvent('test')
    self.server.DeleteEvent('test')
    self.assertFalse(self.server.ListEvents())

  def testListPlayersError(self):
    self.assertRaises(
        ctm_common.NotFoundError,
        self.server.ListPlayers, 'test')

  def testListPlayersEmpty(self):
    self.server.CreateEvent('test')
    expected = []
    actual = self.server.ListPlayers('test')
    self.assertEqual(actual, expected)

  def testSetPlayersError(self):
    players = [ctm_common.Player(0, 'a', None),
               ctm_common.Player(1, 'b', None)]
    self.assertRaises(
        ctm_common.NotFoundError,
        self.server.SetPlayers, 'test', players)

  def testSetPlayers(self):
    self.server.CreateEvent('test')
    players = [ctm_common.Player(0, 'a', None),
               ctm_common.Player(1, 'b', None)]
    self.server.SetPlayers('test', players)
    actual = self.server.ListPlayers('test')
    self.assertEqual(actual, players)

  def testListScansError(self):
    self.assertRaises(
        ctm_common.NotFoundError,
        self.server.ListScans, 'test')

  def testListScansEmpty(self):
    self.server.CreateEvent('test')
    expected = []
    actual = self.server.ListScans('test')
    self.assertEqual(actual, expected)

  def testGetScanEventError(self):
    self.assertRaises(
        ctm_common.NotFoundError,
        self.server.GetScan, 'test', 0)

  def testGetScanScanError(self):
    self.server.CreateEvent('test')
    self.assertRaises(
        ctm_common.NotFoundError,
        self.server.GetScan, 'test', 0)

  def testPostScanError(self):
    self.assertRaises(
        ctm_common.NotFoundError,
        self.server.PostScan, 'test', 'data')

  def testPostScan(self):
    self.server.CreateEvent('test')
    scan_id = self.server.PostScan('test', 'data')
    self.assertTrue(scan_id)

  def testListScans(self):
    self.server.CreateEvent('test')
    scan_id = self.server.PostScan('test', 'data')
    expected = [ctm_common.Scan(scan_id, None, None)]
    actual = self.server.ListScans('test')
    self.assertEqual(actual, expected)

  def testGetScan(self):
    self.server.CreateEvent('test')
    scan_id = self.server.PostScan('test', 'data')
    expected = ctm_common.Scan(scan_id, None, 'data')
    actual = self.server.GetScan('test', scan_id)
    self.assertEqual(actual, expected)

  def testMarkScanEventError(self):
    self.assertRaises(
        ctm_common.NotFoundError,
        self.server.MarkScan, 'test', 0)

  def testMarkScanScanError(self):
    self.server.CreateEvent('test')
    self.assertRaises(
        ctm_common.NotFoundError,
        self.server.MarkScan, 'test', 0)

  def testMarkScan(self):
    self.server.CreateEvent('test')
    self.server.SetPlayers('test', [ctm_common.Player(0, 'a', None)])
    scan_id = self.server.PostScan('test', 'data')
    self.server.MarkScan('test', scan_id, 0)

    expected = ctm_common.Scan(scan_id, 0, 'data')
    actual = self.server.GetScan('test', scan_id)
    self.assertEqual(actual, expected)
    self.assertEqual(
        self.server.ListPlayers('test'),
        [ctm_common.Player(0, 'a', scan_id)])

  def testMarkScanPlayerError(self):
    self.server.CreateEvent('test')
    scan_id = self.server.PostScan('test', 'data')
    self.assertRaises(
        ctm_common.NotFoundError, self.server.MarkScan, 'test', scan_id, 0)

  def testMarkScanClear(self):
    self.server.CreateEvent('test')
    self.server.SetPlayers('test', [ctm_common.Player(0, 'a', None)])
    scan_id = self.server.PostScan('test', 'data')
    self.server.MarkScan('test', scan_id, 0)
    self.server.MarkScan('test', scan_id, None)

    expected = ctm_common.Scan(scan_id, None, 'data')
    actual = self.server.GetScan('test', scan_id)
    self.assertEqual(actual, expected)

  def testMarkScanChange(self):
    self.server.CreateEvent('test')
    self.server.SetPlayers(
        'test', [ctm_common.Player(0, 'a', None),
                 ctm_common.Player(1, 'b', None)])
    scan_id = self.server.PostScan('test', 'data')

    self.server.MarkScan('test', scan_id, 0)
    self.assertEqual(
        self.server.ListPlayers('test'),
        [ctm_common.Player(0, 'a', scan_id),
         ctm_common.Player(1, 'b', None)])

    self.server.MarkScan('test', scan_id, 1)
    self.assertEqual(
        self.server.ListPlayers('test'),
        [ctm_common.Player(0, 'a', None),
         ctm_common.Player(1, 'b', scan_id)])

  def testListScansMarkedAndUnmarked(self):
    self.server.CreateEvent('test')
    self.server.SetPlayers('test', [ctm_common.Player(0, 'a', None)])
    scan_id_0 = self.server.PostScan('test', 'data')
    scan_id_1 = self.server.PostScan('test', 'data')
    self.server.MarkScan('test', scan_id_0, 0)

    expected = [ctm_common.Scan(scan_id_0, 0, None),
                ctm_common.Scan(scan_id_1, None, None)]
    actual = self.server.ListScans('test')
    self.assertEqual(actual, expected)

  def testListScansUnmarkedOnly(self):
    self.server.CreateEvent('test')
    self.server.SetPlayers('test', [ctm_common.Player(0, 'a', None)])
    scan_id_0 = self.server.PostScan('test', 'data')
    scan_id_1 = self.server.PostScan('test', 'data')
    self.server.MarkScan('test', scan_id_0, 0)

    expected = [ctm_common.Scan(scan_id_1, None, None)]
    actual = self.server.ListScans('test', unmarked_only=True)
    self.assertEqual(actual, expected)

if __name__ == '__main__':
    unittest.main()

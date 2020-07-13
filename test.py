import unittest
import taprunner

from influx2csv import utils


class UtilsTest(unittest.TestCase):

	def test(self):
		x = "DUSTBOY/Model-N/NB-IoT/DustBoy-N-023-NB/status"
		self.assertEqual(utils.getDustBoyId(x), "DustBoy-N-023-NB")

	def testGetValueFromDict(self):
		vv = {"key": "topic", "value": "DUSTBOY/Model-T/6015/DustboyV1015/status"}
		self.assertEqual(utils.getTopicValue(vv),
						 "DUSTBOY/Model-T/6015/DustboyV1015/status")

	def testZCalculateTomorrow(self):
		self.assertEqual(utils.tomorrow("2020-04-01"), "2020-04-02")
		self.assertEqual(utils.tomorrow("2020-04-30"), "2020-05-01")
		self.assertEqual(utils.tomorrow("2020-02-28"), "2020-02-29")
		self.assertEqual(utils.tomorrow("2020-02-29"), "2020-03-01")
		self.assertEqual(utils.tomorrow("2020-12-31"), "2021-01-01")

	def testSubtractList(self):
		a = ['dustboydb', 'kadyaidb', 'aqithaidb', 'aqithaicom_db', 'laris1db', 'dustboy2_nbiotdb', 'dustboy']
		b = ['kadyaidb', 'laris1db']
		c = ['dustboydb', 'aqithaidb', 'aqithaicom_db', 'dustboy2_nbiotdb', 'dustboy']

		self.assertEqual(utils.exclude(a, b), c)

	def testSplitToDict(self):
		shfile = '/Users/nat/ccdc/scripts/dustboy2_nbiotdb_-_dustboy2_nbiotdb_-_dustboy2-02_-_2020-04-20'
		self.assertEqual(utils.getDictInfo(shfile),
						 {'date': '2020-04-20', 'datedir': '2020/04', 'measurement': 'dustboy2_nbiotdb',
						  'database': 'dustboy2_nbiotdb',
						  'nickname': 'dustboy2-02'})


if __name__ == '__main__':
	unittest.main(testRunner=taprunner.TAPTestRunner())  # output='test-reports'

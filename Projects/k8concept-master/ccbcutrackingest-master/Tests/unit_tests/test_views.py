import pytest
import unittest
import os
import sys

sys.path.append(os.path.join(os.getcwd(), 'Application'))
os.environ["APPINSIGHTS_INSTRUMENTATIONKEY"] = os.path.join(os.getcwd(), 'Application')

class ViewTest(unittest.TestCase):

	def setUp(self):
		# app.config['TESTING'] = True
		# self.app = app.test_client()
		return

	def test_unit_home(self):
		response = self.app.get('/getanalysisresultspages')
		self.assertEqual(response.status_code, 200)


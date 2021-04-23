import unittest
from DataStore import DataStore
import constants
from io import StringIO
import sys
import time
import json
import os

class testUnit(unittest.TestCase):

	def setUp(self):
		self.ds = DataStore()
	
	def tearDown(self):
		if os.path.exists("store.json"):
			os.remove("store.json")
		
		if self.ds.path in self.ds.working_paths:
			self.ds.working_paths.remove(self.ds.path)


	def test_create(self):
		self.ds.create("a",'{"x":"y"}',10)
		time.sleep(1)
		exact_time = self.ds.store['a']['time']
		sample_dict = {"a": {"time": exact_time, "data": {"x": "y"}}}
		self.assertDictEqual(self.ds.store,sample_dict)

	def test_delete_key_not_present(self):
		capturedOutput = StringIO()
		sys.stdout = capturedOutput
		self.ds.delete('k')
		sys.stdout = sys.__stdout__
		self.assertEqual(capturedOutput.getvalue(),constants.KEY_NOT_PRESENT_ERROR+'\n')
		
	def test_read_key_expired(self):
		self.ds.create("b",'{"x":"y"}',1)
		time.sleep(2)
		capturedOutput = StringIO()
		sys.stdout = capturedOutput
		self.ds.read('b')
		sys.stdout = sys.__stdout__
		self.assertEqual(capturedOutput.getvalue(),constants.KEY_EXPIRED_ERROR+'\n')

	def test_read_key_not_present(self):
		capturedOutput = StringIO()
		sys.stdout = capturedOutput
		self.ds.read('z')
		sys.stdout = sys.__stdout__
		self.assertEqual(capturedOutput.getvalue(),constants.KEY_NOT_PRESENT_ERROR+'\n')

	def test_create_key_inv(self):
		capturedOutput = StringIO()
		sys.stdout = capturedOutput
		self.ds.create("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",'{"x":"y"}',1)
		sys.stdout = sys.__stdout__
		self.assertEqual(capturedOutput.getvalue(),constants.KEY_INVALID_ERROR+'\n')
 
	def test_create_not_valid_json(self):
		capturedOutput = StringIO()
		sys.stdout = capturedOutput
		self.ds.create('c','{"x":st}',1)
		sys.stdout = sys.__stdout__
		self.assertEqual(capturedOutput.getvalue(),constants.VALUE_FORMAT_ERROR+'\n')
  

	def test_create_inv_val(self):
		capturedOutput = StringIO()
		sys.stdout = capturedOutput
		st="abcd"
		with open("test_data/big_data.json") as fp:
			d = json.load(fp)
		d = json.dumps(d)
		self.ds.create(st,d,1)
		sys.stdout = sys.__stdout__
		self.assertEqual(capturedOutput.getvalue(),constants.VALUE_LENGTH_ERROR+'\n')

if __name__ == '__main__':
    unittest.main()

import os
import json
import time
import constants
from threading import Thread, Lock
from datetime import datetime, timedelta

class DataStore(object):
	working_paths = set()

	def __init__(self, path="store.json"):
		self.path = path

		# Allowing only one instance at a path
		if self.path in self.working_paths:
			raise Exception(constants.MULTIPLE_INSTANCE_EXCEPTION)
		self.working_paths.add(self.path)

		# Checking if file exists and is valid
		if not self.__is_non_zero_file(self.path):
			with open(self.path, 'w+') as f:
				f.write("{}")
				f.close()
		
		# Loading the file in the store and making the lock
		with open(self.path) as f:
			self.store = json.load(f)
		self.lock = Lock()

	def __del__(self):
		if self.path in self.working_paths:
			self.working_paths.remove(self.path)

	def create(self, key, value, offsettime):
		Thread(target=self.__create_subprocess,args=(key,value,offsettime)).start()

	def read(self,key):
		Thread(target=self.__read_subprocess,args=(key)).start()

	def delete(self, key):
		Thread(target=self.__delete_subprocess,args=(key)).start()

	def __delete_subprocess(self, key):
		self.lock.acquire()
		if key not in self.store:
			print(constants.KEY_NOT_PRESENT_ERROR)
			return
		del self.store[key]
		self.lock.release()

	def __create_subprocess(self, key, value, offsettime):
		# Checking Key Validity
		if type(key)!= str or len(key)>32:
			print(constants.KEY_INVALID_ERROR)
			return

		# Checking Data Validity
		try:
			acquired_data = json.loads(value)
			if len(json.dumps(acquired_data))>(16*1024):
				print(constants.VALUE_LENGTH_ERROR)
				return
		except:
			print(constants.VALUE_FORMAT_ERROR)
			return	

		# Checking File Size
		if self.__exceeds_file_size_limit():
			print(constants.FILE_LIMIT_EXCEEDED_EXCEPTION)
			return

		self.lock.acquire()
		if key in self.store:
			if self.store[key]["time"]<self.__get_time():
				del self.store[key]
			else:
				print(constants.KEY_ALREADY_PRESENT_ERROR)
				self.lock.release()
				return

		self.store[key] = {"time": self.__get_time(offsettime),"data":json.loads(value)}
		with open(self.path, 'w') as fp:
			json.dump(self.store, fp)
			fp.close()
		self.lock.release()

	def __read_subprocess(self,key):
		self.lock.acquire()
		if key not in self.store:
			print(constants.KEY_NOT_PRESENT_ERROR)
			self.lock.release()
			return
		
		if self.store[key]["time"]<self.__get_time():
			print(constants.KEY_EXPIRED_ERROR)
			self.lock.release()
			self.delete(key)
			return

		data = self.store[key]["data"]
		self.lock.release()

		print("Data for Key ({}):".format(key),data)

	def __exceeds_file_size_limit(self):
		if os.path.getsize(self.path)>(1024*1024*1024):
			return True
		return False


	def __is_non_zero_file(self, fpath):  
		return os.path.isfile(fpath) and os.path.getsize(fpath) > 0

	def __get_time(self, offset=0):
		final_time = datetime.now() + timedelta(seconds=offset)
		return final_time.isoformat()

if __name__ == '__main__':
	
	if os.path.exists("store.json"):
		os.remove("store.json")

	dStore = DataStore()
	dStore.create("a",'{"x":"y"}',5)
	time.sleep(3)
	dStore.read("a")
	time.sleep(3)
	dStore.read("a")
	time.sleep(1)
	dStore.create("a",'{"y":"z"}',5)
	time.sleep(1)
	dStore.read("a")
	dStore.create("a",'{"x":"y"}',5)
	time.sleep(1)
	dStore.delete("a")
	time.sleep(1)
	dStore.read("a")


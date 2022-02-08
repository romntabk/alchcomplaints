import requests
import time
from datetime import date, timedelta
from data_base import AlchDataBase
import json # delete
import timer as my_timer
class downloader:
	def __init__ (self, TIME_INTERVAL = 31):
		self.data_downloaded = False
		self.URL = 'https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/?format=json&date_received_min=' # yyyy-mm-dd
		self.TIME_INTERVAL = TIME_INTERVAL
		self.json_data = None

		# self.__table_exist=True
	def __table_exist(self): # TODO: избавиться от этого метода
		
		return True
		pass

	@my_timer.timer('download')
	def download_data(self): # TODO: 31 асинхронный запрос для каждого дня + logging 
		if self.__table_exist():	
			date_received_min = date.today()-timedelta(self.TIME_INTERVAL) 
			try:
				response = requests.get(f'{self.URL}{date_received_min}') 
				self.json_data = response.json()
				self.data_downloaded = True
			except: # TODO: добавить дополнительные попытки + timeout, вдруг отвалится + logging
				pass
				print('aaa')
		return
		# download all


if __name__ == '__main__':
	d = downloader(31)
	d.download_data()
	data_json = d.json_data
	# file_jsn_read  = open('stop_requests10.json','r')
	try:
		# data_json= json.load(file_jsn_read)
		db = AlchDataBase()
		db.add_and_parse_json(data_json)
		db.draw_chart(1)
	finally:
		pass
	

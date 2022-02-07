import requests
import time
from datetime import date, timedelta
import file_3 as my_db
import json # delete
import file_5 as my_timer
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
	# d = downloader(31)
	# d.download_data()
	# data_json = d.json_data
	# print(len(data_json))
	# file_jsn = open('stop_requests10.json','w')
	# json.dump(data_json,file_jsn)
	file_jsn_read  = open('stop_requests10.json','r')
	# file_jsn_read  = open('complaints.json','r')
	data_json= json.load(file_jsn_read)
	# print(jsn) 
	# print(len(data_json))
	# print(len(data_json))
	# print(data_json[0])
	db = my_db.AlchDataBase()
	# print(db.select_last_month()
	# print(data_jso)
	# print(data_json[0])
	db.add_and_parse_json(data_json)	
	# db.do_fun()
 

	# проверка существует ли таблица.
	#    да - скачать последний 1 месяц.
	#   нет - скачать бд  	
	# if table _exist:
	# 	a = requests.get(url)
	# print(a)
	# print(a.text)
	# print('----',)
	# print(a.json())
	# print(len(a.json()))
	# help(a)


#1
#2
#3
#
#n


#1
#2
#	
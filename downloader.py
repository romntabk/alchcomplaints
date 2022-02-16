import requests
import timer as my_timer
from datetime import date, timedelta

class Downloader:
	def __init__ (self, TIME_INTERVAL=31, session=None):
		self.URL = ('https://www.consumerfinance.gov/data-research/'
				   'consumer-complaints/search/api/v1/'
				   '?format=json&date_received_min=') # yyyy-mm-dd
		self.URL_FULL_DATA = ('https://files.consumerfinance.gov/ccdb/'
							  'complaints.json')
		self.TIME_INTERVAL = TIME_INTERVAL


	@my_timer.timer('download monthly data')
	def download_monthly_data(self): 
		''' Uploads complaints only for the last month '''	
		
		date_received_min = date.today() - timedelta(self.TIME_INTERVAL) 
		try:
			response = requests.get(f'{self.URL}{date_received_min}') 
			return response.json()
		except: # TODO: добавить дополнительные попытки + timeout, вдруг отвалится + logging
			pass
	
	@my_timer.timer('Initial download')
	def download_initial_data(self):   # очень долго загружается.. 2.5ГБ ОЗУ
		''' Loads the entire database '''
		
		try: 
			response = requests.get(self.URL_FULL_DATA)
			return response.json()
		except: # TODO: добавить дополнительные попытки + timeout, вдруг отвалится + logging
			pass
		
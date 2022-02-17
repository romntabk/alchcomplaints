from timer import timer

import requests
from datetime import date, timedelta
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


def request_retry(retries=3, backoff_factor=0.5, status_forcelist=(500, 502, 504)):
	retry = Retry(
		total = retries, 
		read = retries,
		connect = retries,
		backoff_factor = backoff_factor,
		status_forcelist = status_forcelist
		)
	adapter = HTTPAdapter(max_retries=retry)
	session = requests.Session()
	session.mount('http://', adapter)
	session.mount('https://', adapter)
	return session


class Downloader:

	URL = ('https://www.consumerfinance.gov/data-research/'
		   'consumer-complaints/search/api/v1/'
		   '?format=json&date_received_min=') # yyyy-mm-dd
	URL_FULL_DATA = ('https://files.consumerfinance.gov/ccdb/'
					 'complaints.json')


	def __init__ (self, time_interval=31, session=None):
		self.time_interval = time_interval


	@timer('Downloading data for the last month')
	def download_monthly_data(self): 
		''' Uploads complaints only for the last month '''	
		
		date_received_min = date.today() - timedelta(self.time_interval) 
		try:
			response = (request_retry()
				.get(
					f'{Downloader.URL}{date_received_min}',
					timeout=10
					)
				)
			return response.json()
		except Exception as x: 
			raise Exception(
				(f'Failed to download data for the last month: ' 
				 f'{x.__class__.__name__}')
				)
		
	
	@timer('Downloading the initial data')
	def download_initial_data(self): 
		''' Loads the entire database '''
		
		try: 
			response = (request_retry()
				.get(
					Downloader.URL_FULL_DATA, 
					timeout=10
					)
				)
			return response.json()
		except Exception as x:
			raise Exception(f'Failed to download initial data: {x.__class__.__name__}')
		
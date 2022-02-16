import requests
import timer as my_timer
from datetime import date, timedelta
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


def request_retry(retries=3, backoff_factor=0.5,status_forcelist=(500,502,504)):
	retry = Retry(
		total=retries, 
		read=retries,
		connect=retries,
		backoff_factor=backoff_factor,
		status_forcelist=status_forcelist
		)
	adapter = HTTPAdapter(max_retries=retry)
	session=requests.Session()
	session.mount('http://',adapter)
	session.mount('https://',adapter)
	return session


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
			response = request_retry().get(
				f'{self.URL}{date_received_min}',
				timeout=10
				) 
			return response.json()
		except Exception as x: 
			raise Exception(
				f'Failed to load monthly data: {x.__class__.__name__}')
		
	
	@my_timer.timer('Initial download')
	def download_initial_data(self): 
		''' Loads the entire database '''
		
		try: 
			response = request_retry().get(self.URL_FULL_DATA, timeout=10)
			return response.json()
		except Exception as x:
			raise Exception(f'Failed to load full data: {x.__class__.__name__}')
		
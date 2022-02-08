import requests
import time
from datetime import date, timedelta
from data_base import AlchDataBase
import json # delete
import timer as my_timer




if __name__ == '__main__':
	try:
		db = AlchDataBase()
		db.load_changes()
		db.draw_chart_new_change()
		db.draw_chart_company('Aargon Agency, Inc.','Ability Recovery Services, LLC')
	finally:
		pass
	

from data_base import AlchDataBase


def main():
	try:
		db = AlchDataBase()
		db.load_changes()
		db.draw_chart_new_change()
		db.draw_chart_company('Aargon Agency, Inc.','Ability Recovery Services, LLC')
	except Exception as x:
		print(repr(x))

if __name__ == '__main__': # TODO: add argparse 
	main()
	
  
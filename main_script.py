from data_base import AlchDataBase


def main():
	db = AlchDataBase()
	db.load_changes()
	db.draw_chart_new_change()
	db.draw_chart_company('Aargon Agency, Inc.','Ability Recovery Services, LLC')
	

if __name__ == '__main__': # TODO: add argparse 
	main()
	

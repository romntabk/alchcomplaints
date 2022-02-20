from data_base import DBContextManager


def main():
	try:
		with DBContextManager() as db:
			# info = db.load_changes()
			# print(info)
			db.draw_chart_new_change()
			# db.draw_chart_company('Aargon Agency, Inc.', 'Ability Recovery Services, LLC')
	except Exception as x:
		print(repr(x))


if __name__ == '__main__': 
	main() 
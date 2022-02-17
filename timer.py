import time

def timer(text): # не забыть удалить
	def decorator(func):
		def wrapper(*args, **kwargs):
			time_ = time.time()
			print('-' * 5, text)
			try:
				result = func(*args, **kwargs)
			finally:
				print('-' * 5, text, 'Execution time: ', time.time() - time_)
			return result
		return wrapper
	return decorator





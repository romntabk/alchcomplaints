import time
def timer(text): # не забыть удалить
	def decorator(func):
		def wrapper(*args,**kwargs):
			time_ = time.time()
			result = func(*args,**kwargs)
			print('-'*10, text, 'Execution time: ', time.time()-time_)
			return result
		return wrapper
	return decorator


from collections import defaultdict





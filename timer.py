import time
from functools import wraps

def timer(text, offset=[0]):
	def decorator(func):
		@wraps(func)
		def wrapper(*args, **kwargs):
			time_ = time.time()
			print(f'{" " * offset[0]} {text} starts')
			try:
				offset[0] += 5
				result = func(*args, **kwargs)
			finally:
				offset[0] -= 5
				print(
					(f'{" " * offset[0]} {text} ' 
				     f'Execution time: {round(time.time() - time_, 2)}s.')
					)
			return result
		return wrapper
	return decorator





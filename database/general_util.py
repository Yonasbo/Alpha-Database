import time
from functools import wraps

def timeme(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        a = time.time()
        result = func(*args, **kwargs)
        print(f"@timeme: {func.__name__} took {time.time()-a} seconds")
        return result
    return wrapper



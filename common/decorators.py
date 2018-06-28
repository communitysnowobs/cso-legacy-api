from functools import wraps
import threading
import time

def unsafe(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            return None
    return wrapper

def batch(size=16):
    def decorator(func):
        @wraps(func)
        def wrapper(l):
            assert isinstance(l, list), "data has to be in list form."
            results = []
            for i in range(0, len(l), size):
                result = func(l[i:i + size])
                results.extend(result)
            return results
        return wrapper
    return decorator

def threaded(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        thread = threading.Thread(target=func, args=args, kwargs=kwargs)
        thread.start()
        return thread
    return wrapper

def locked(func):
    lock = threading.Lock()
    @wraps(func)
    def wrapper(*args, **kwargs):
        lock.acquire()
        res = func(*args, **kwargs)
        lock.release()
        return res
    return wrapper

def cache(ttl=60, max_size=128):

    hash_separator = object()
    cache = {}
    queue = []

    def insert(key, val):
        # Make sure cache does not exceed max_size
        while len(cache) >= max_size or len(queue) >= max_size:
            remove()
        cache[key] = (time.time(), val)
        queue.append(key)

    def update(key, val):
        # Move key to end of queue and update value
        if key in queue:
            index = queue.index(key)
            del queue[index]
        queue.append(key)
        cache[key] = (time.time(), val)

    def remove():
        # Remove oldest key from cache
        key = queue[0]
        del queue[0]
        del cache[key]

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Get unique key based on arguments passed to function
            key = hash(args + (hash_separator,) + tuple(sorted(kwargs.items())))
            if key in cache:
                cached_at, val = cache.get(key)
                if time.time() - cached_at > ttl:
                    new_val = func(*args, **kwargs)
                    update(key, new_val)
                    return new_val
                else:
                    return val
            else:
                val = func(*args, **kwargs)
                insert(key, val)
                return val

        return wrapper
    return decorator

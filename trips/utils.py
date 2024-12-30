import time


def timeit(func):
    """
    A decorator to print how long a function took to execute.
    """

    def wrapper(*args, **kwargs):
        """
        Wrapper function to track execution time of the decorated function.

        Prints to stdout the execution time of the decorated function.
        """

        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        print(f"{func.__name__} took {end - start} seconds")
        return result

    return wrapper

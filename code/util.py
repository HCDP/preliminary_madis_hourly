from time import sleep

#create generic retry for passed function
def handle_retry(f, args, failure_handler = None, failure_args = (), max_retries = 10, retry = 0):
    sleep(retry**2)
    try:
        return f(*args)
    except Exception as e:
        if retry < max_retries:
            print(f"{f.__name__} attempt {retry} failed with error: {e}. Retrying...")
            if failure_handler is not None:
                failure_handler(*failure_args)
            return handle_retry(f, args, failure_handler, failure_args, max_retries, retry + 1)
        else:
            raise e
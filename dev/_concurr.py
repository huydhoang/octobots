import concurrent.futures
import itertools
import time

max_workers = 4

tasks_to_do = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30]
iterator = iter(tasks_to_do)

def perform(task):
    time_start = time.time()
    print(f"Number {task} started.")
    time.sleep(10)
    # print(f"Number {task} finished.")
    return time_start

with concurrent.futures.ThreadPoolExecutor() as executor:

    # Schedule the first N futures.  We don't want to schedule them all
    # at once, to avoid consuming excessive amounts of memory.
    futures = {
        executor.submit(perform, task): task
        for task in itertools.islice(iterator, max_workers)
    }

    while futures:
        # Wait for the next future to complete.
        done, _ = concurrent.futures.wait(
            futures, return_when=concurrent.futures.FIRST_COMPLETED
        )

        for fut in done:
            original_task = futures.pop(fut)
            print(f"Number {original_task} done in {round(time.time() - fut.result(), 1)} seconds.")

        # Schedule the next set of futures.  We don't want more than N futures
        # in the pool at a time, to keep memory consumption down.
        for task in itertools.islice(iterator, len(done)):
            fut = executor.submit(perform, task)
            futures[fut] = task
            print(f"Active workers: {len(futures)}")
            print(list(futures.values()))
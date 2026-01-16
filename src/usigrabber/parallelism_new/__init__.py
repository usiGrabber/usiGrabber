import logging
import logging.config
import multiprocessing as mp
import time
from collections import deque

from usigrabber.parallelism_new import logging_listener, worker

config_initial = {
    "version": 1,
    "handlers": {"console": {"class": "logging.StreamHandler", "level": "INFO"}},
    "root": {"handlers": ["console"], "level": "DEBUG"},
}


def main():
    start_time = time.time()

    logging.config.dictConfig(config_initial)
    logger = logging.getLogger("setup")

    queue = mp.Queue()

    # start logging listener process
    listener = mp.Process(target=logging_listener.listener_process, args=(queue,))
    listener.start()

    # 2. Configure the Executor
    # Note: We pass functions FROM the tasks module
    with mp.Pool(processes=10, initializer=worker.init_worker_queue, initargs=(queue,)) as pool:
        it = pool.imap_unordered(worker.do_work, range(1000))

        # This "drains" the generator immediately but lazily
        # maxlen=0 means it stores nothing in memory
        deque(it, maxlen=0)

    queue.put_nowait(None)  # send stop signal to logging listener
    listener.join()  # wait for listener to finish

    logger.info(f"All done in {time.time() - start_time:.2f} seconds.")


if __name__ == "__main__":
    main()

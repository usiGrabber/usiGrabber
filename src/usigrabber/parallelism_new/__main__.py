import logging
import logging.config
import multiprocessing as mp
import os
import time
from collections import deque

from usigrabber.parallelism_new import logging_listener, pride, worker

WORKER_COUNT = max(1, int(os.getenv("USIGRABBER_NUM_WORKERS", mp.cpu_count() - 1)))

config_initial = {
    "version": 1,
    "handlers": {
        "console": {
            "()": "rich.logging.RichHandler",
            "rich_tracebacks": True,
            "markup": True,
            "level": "INFO",
        }
    },
    "root": {"handlers": ["console"], "level": "DEBUG"},
}


def main():
    start_time = time.time()

    logging.config.dictConfig(config_initial)
    logger = logging.getLogger("setup")

    queue = mp.Queue()

    try:
        # start logging listener process
        listener = mp.Process(target=logging_listener.listener_process, args=(queue,))
        listener.start()

        # configure the execution pool
        with mp.Pool(
            processes=WORKER_COUNT,
            initializer=worker.init_worker_queue,
            initargs=(queue,),
        ) as pool:
            it = pool.imap_unordered(
                worker.do_work,
                pride.dummy_generator(),
                chunksize=10,
            )

            # This "drains" the generator immediately but lazily
            # maxlen=0 means it stores nothing in memory
            deque(it, maxlen=0)

        logger.info(f"All done in {time.time() - start_time:.2f} seconds.")
    except KeyboardInterrupt:
        logger.warning("Caught KeyboardInterrupt, terminating workers...")
        pool.terminate()
    finally:
        queue.put_nowait(None)  # send stop signal to logging listener
        listener.join()  # wait for listener to finish


if __name__ == "__main__":
    main()

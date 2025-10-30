from utils import get_logger
from crawler.frontier import Frontier
from crawler.worker import Worker

class Crawler(object):
    def __init__(self, config, restart, frontier_factory=Frontier, worker_factory=Worker):
        self.config = config
        self.logger = get_logger("CRAWLER")
        self.frontier = frontier_factory(config, restart)
        self.workers = list()
        self.worker_factory = worker_factory

    def start_async(self):
        # If only one thread is requested, avoid spawning a new Thread to
        # keep all shelve (sqlite) access in the same thread that opened it.
        if self.config.threads_count == 1:
            worker = self.worker_factory(0, self.config, self.frontier)
            # Run synchronously in the main thread to avoid sqlite thread errors
            worker.run()
            self.workers = [worker]
            return

        self.workers = [
            self.worker_factory(worker_id, self.config, self.frontier)
            for worker_id in range(self.config.threads_count)]
        for worker in self.workers:
            worker.start()

    def start(self):
        self.start_async()
        self.join()

    def join(self):
        # When running synchronously (THREADCOUNT == 1), there is no separate
        # thread to join. Only join when threads were actually started.
        if self.config.threads_count == 1:
            return
        for worker in self.workers:
            worker.join()

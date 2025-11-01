from threading import Thread

from inspect import getsource
from utils.download import download
from utils import get_logger
import scraper
import time
from urllib.parse import urlparse

# Per-domain politeness tracking (single-threaded worker)
_last_fetch_by_host = {}


class Worker(Thread):
    def __init__(self, worker_id, config, frontier):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)
        
    def run(self):
        while True:
            tbd_url = self.frontier.get_tbd_url()
            if not tbd_url:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break
            # Enforce per-domain politeness
            try:
                host = (urlparse(tbd_url).hostname or "").lower()
                now = time.time()
                last = _last_fetch_by_host.get(host)
                sleep_needed = 0.0
                if last is not None:
                    delta = now - last
                    need = self.config.time_delay - delta
                    if need > 0:
                        sleep_needed = need
                if sleep_needed > 0:
                    time.sleep(sleep_needed)
                # Record start time of this request for the host
                _last_fetch_by_host[host] = time.time()
            except Exception:
                pass
            resp = download(tbd_url, self.config, self.logger)
            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
            scraped_urls = scraper.scraper(tbd_url, resp)
            for scraped_url in scraped_urls:
                self.frontier.add_url(scraped_url)
            self.frontier.mark_url_complete(tbd_url)

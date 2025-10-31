import os
import re
import threading
from collections import Counter
from urllib.parse import urldefrag, urlparse

from bs4 import BeautifulSoup


_stopwords = {
	"a","about","above","after","again","against","all","am","an","and","any","are","aren't","as","at",
	"be","because","been","before","being","below","between","both","but","by",
	"can't","cannot","could","couldn't",
	"did","didn't","do","does","doesn't","doing","don't","down","during",
	"each",
	"few","for","from","further",
	"had","hadn't","has","hasn't","have","haven't","having","he","he'd","he'll","he's","her","here","here's","hers","herself","him","himself","his","how","how's",
	"i","i'd","i'll","i'm","i've","if","in","into","is","isn't","it","it's","its","itself",
	"let's",
	"me","more","most","mustn't","my","myself",
	"no","nor","not","of","off","on","once","only","or","other","ought","our","ours","ourselves","out","over","own",
	"same","shan't","she","she'd","she'll","she's","should","shouldn't","so","some","such",
	"than","that","that's","the","their","theirs","them","themselves","then","there","there's","these","they","they'd","they'll","they're","they've","this","those","through","to","too",
	"under","until","up",
	"very",
	"was","wasn't","we","we'd","we'll","we're","we've","were","weren't","what","what's","when","when's","where","where's","which","while","who","who's","whom","why","why's","with","won't","would","wouldn't",
	"you","you'd","you'll","you're","you've","your","yours","yourself","yourselves"
}

_word_re = re.compile(r"[a-zA-Z0-9']+")


class Analytics:
	def __init__(self):
		self._lock = threading.Lock()
		self.unique_urls = set()
		self.word_counts = Counter()
		self.longest_page_url = None
		self.longest_page_word_count = 0
		self.subdomain_counts = Counter()
		self.output_dir = os.path.join("Logs", "analytics")
		os.makedirs(self.output_dir, exist_ok=True)

	def _tokenize(self, text: str):
		for token in _word_re.findall(text.lower()):
			# strip leading/trailing apostrophes
			t = token.strip("'")
			if t and t not in _stopwords and any(c.isalpha() for c in t):
				yield t

	def record_page(self, url: str, html_bytes: bytes):
		if not url:
			return
		# defragment URL for uniqueness
		url_no_frag, _ = urldefrag(url)
		parsed = urlparse(url_no_frag)
		# ignore non-uci domains for subdomain stats
		host = parsed.hostname or ""
		with self._lock:
			self.unique_urls.add(url_no_frag)
			if host.endswith(".uci.edu"):
				self.subdomain_counts[host] += 1

		# Parse content and update word stats
		if not html_bytes:
			return
		try:
			soup = BeautifulSoup(html_bytes, "html.parser")
			text = soup.get_text(" ", strip=True)
		except Exception:
			return

		words = list(self._tokenize(text))
		word_count = len(words)
		if word_count == 0:
			return
		with self._lock:
			self.word_counts.update(words)
			if word_count > self.longest_page_word_count:
				self.longest_page_word_count = word_count
				self.longest_page_url = url_no_frag

	def write_reports(self):
		with self._lock:
			# Unique pages
			unique_path = os.path.join(self.output_dir, "unique_pages.txt")
			with open(unique_path, "w", encoding="utf-8") as f:
				f.write(f"count: {len(self.unique_urls)}\n")

			# Longest page
			longest_path = os.path.join(self.output_dir, "longest_page.txt")
			with open(longest_path, "w", encoding="utf-8") as f:
				f.write(f"url: {self.longest_page_url or ''}\n")
				f.write(f"word_count: {self.longest_page_word_count}\n")

			# Top 50 words
			top_words_path = os.path.join(self.output_dir, "top_50_words.txt")
			with open(top_words_path, "w", encoding="utf-8") as f:
				for word, cnt in self.word_counts.most_common(50):
					f.write(f"{word}, {cnt}\n")

			# Subdomains
			subdomains_path = os.path.join(self.output_dir, "subdomains.txt")
			with open(subdomains_path, "w", encoding="utf-8") as f:
				for subdomain in sorted(self.subdomain_counts.keys()):
					f.write(f"{subdomain}, {self.subdomain_counts[subdomain]}\n")


analytics = Analytics()


def record_page(url: str, html_bytes: bytes):
	analytics.record_page(url, html_bytes)
	analytics.write_reports()



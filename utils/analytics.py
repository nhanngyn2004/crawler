# utils/analytics.py

import os
import re
import threading
from collections import Counter
from urllib.parse import urldefrag, urlparse

from bs4 import BeautifulSoup  # pip install beautifulsoup4

# ---------------- Configuration ----------------
OUTPUT_DIR = os.path.join("Logs", "analytics")
FLUSH_EVERY = 50  # write to disk every N pages to reduce I/O

# English stopwords (reasonable fixed set per spec)
_STOPWORDS = {
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

_WORD_RE = re.compile(r"[a-zA-Z0-9']+")


class Analytics:
    """
    Thread-safe analytics for the assignment report:

    - Unique pages: counted by URL with fragment removed (and nothing else).
    - Longest page: by word count of visible text (no HTML markup).
    - Top 50 words: frequency over all pages (stopwords removed).
    - Subdomains: counts of unique pages per subdomain under uci.edu.
    """
    def __init__(self, output_dir: str = OUTPUT_DIR, flush_every: int = FLUSH_EVERY):
        self._lock = threading.Lock()

        # Unique URLs (defragmented only — EXACTLY per spec)
        self.unique_urls = set()

        # Word stats
        self.word_counts = Counter()
        self.longest_page_url = None
        self.longest_page_word_count = 0

        # Subdomain -> unique page count (only *.uci.edu)
        self.subdomain_counts = Counter()

        # I/O
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        self._pages_since_flush = 0
        self._flush_every = max(1, int(flush_every))

    # ---------- text helpers ----------
    def _extract_visible_text(self, page_html_or_text):
        """Accepts bytes or str (HTML); returns visible text string."""
        if page_html_or_text is None:
            return ""
        # Ensure str
        if isinstance(page_html_or_text, (bytes, bytearray)):
            try:
                page_html_or_text = page_html_or_text.decode("utf-8", errors="replace")
            except Exception:
                page_html_or_text = page_html_or_text.decode("latin-1", errors="replace")

        soup = BeautifulSoup(page_html_or_text, "html.parser")
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        return soup.get_text(separator=" ", strip=True)

    def _tokenize_no_stop(self, text: str):
        """Yield tokens (letters/digits), lowercased, stopwords removed; must contain a letter."""
        for token in _WORD_RE.findall(text.lower()):
            t = token.strip("'")
            if t and any(c.isalpha() for c in t) and t not in _STOPWORDS:
                yield t

    def _count_words_total(self, text: str) -> int:
        """Count 'word' tokens (no stopword removal) for longest-page metric; must contain a letter."""
        cnt = 0
        for token in _WORD_RE.findall(text.lower()):
            t = token.strip("'")
            if t and any(c.isalpha() for c in t):
                cnt += 1
        return cnt

    # ---------- public API ----------
    def record_page(self, url: str, page_html_or_text):
        """
        Record analytics for a single fetched page.
        - url: the final URL as seen by the crawler (string)
        - page_html_or_text: page bytes or HTML string
        """
        if not url:
            return

        # Per spec: uniqueness = URL with fragment removed ONLY
        url_no_frag, _ = urldefrag(url)
        parsed = urlparse(url_no_frag)
        host = (parsed.hostname or "").lower()

        # Register unique URL & subdomain counts
        with self._lock:
            is_new = url_no_frag not in self.unique_urls
            if is_new:
                self.unique_urls.add(url_no_frag)
                if host.endswith(".uci.edu"):
                    self.subdomain_counts[host] += 1

        # Extract visible text for word stats
        text = self._extract_visible_text(page_html_or_text)
        if not text:
            return

        # Word frequency (stopwords removed)
        tokens = list(self._tokenize_no_stop(text))

        # Longest page (total word tokens, no stopword filtering)
        word_count = self._count_words_total(text)

        # Update shared state
        with self._lock:
            if tokens:
                self.word_counts.update(tokens)
            if word_count > self.longest_page_word_count:
                self.longest_page_word_count = word_count
                self.longest_page_url = url_no_frag

            # Buffered writes
            self._pages_since_flush += 1
            if self._pages_since_flush >= self._flush_every:
                self._pages_since_flush = 0
                self._write_reports_nolock()

    def write_reports(self):
        """Force-write all reports to disk."""
        with self._lock:
            self._write_reports_nolock()

    # ---------- internal I/O ----------
    def _write_reports_nolock(self):
        # 1) Unique pages
        with open(os.path.join(self.output_dir, "unique_pages.txt"), "w", encoding="utf-8") as f:
            f.write(f"count: {len(self.unique_urls)}\n")

        # 2) Longest page
        with open(os.path.join(self.output_dir, "longest_page.txt"), "w", encoding="utf-8") as f:
            f.write(f"url: {self.longest_page_url or ''}\n")
            f.write(f"word_count: {self.longest_page_word_count}\n")

        # 3) Top 50 words (word, count)
        with open(os.path.join(self.output_dir, "top_50_words.txt"), "w", encoding="utf-8") as f:
            for word, cnt in self.word_counts.most_common(50):
                f.write(f"{word}, {cnt}\n")

        # 4) Subdomains (sorted alphabetically)
        with open(os.path.join(self.output_dir, "subdomains.txt"), "w", encoding="utf-8") as f:
            for sub in sorted(self.subdomain_counts.keys()):
                f.write(f"{sub}, {self.subdomain_counts[sub]}\n")


# --------------- module-level singleton + wrappers ---------------
analytics = Analytics()


def record_page(url: str, page_html_or_text):
    """
    Public function used by the scraper:
    - Pass in the final URL and the raw HTML (bytes) or text.
    - This function is safe to call from multiple threads.
    """
    analytics.record_page(url, page_html_or_text)


def flush_analytics():
    """
    Optional: call once at shutdown to force a final write.
    If you don't call it, periodic flushes will still write during the crawl.
    """
    analytics.write_reports()

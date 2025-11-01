import re
from urllib.parse import urlparse, urljoin, urldefrag, parse_qsl, urlencode, urlunparse
from bs4 import BeautifulSoup
from utils.analytics import record_page

# ---------- Additions: safe canonicalization + trap helpers ----------
QUERY_PARAM_BLACKLIST = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "gclid", "fbclid", "mc_cid", "mc_eid",
    "replytocom", "share", "sessionid", "phpsessid", "sid", "sessid",
    "amp", "amp_html",
}

TRAP_SUBSTRINGS_PATH = [
    "/calendar", "/ical", "/feed/", "/wp-json", "/wp-admin", "/xmlrpc.php",
    "/tag/", "/author/", "/comments", "/embed", "/print/", "/share/", "/login",
    "/logout", "/signup", "/cgi-bin", "/redirect",
]

TRAP_SUBSTRINGS_QUERY = [
    "format=xml", "format=amp", "action=", "sort=", "filter=", "feed=",
    "C=;O=", "C=N;O=D", "C=M;O=A",
]

REPEATED_SEGMENT_RE = re.compile(r"(/[^/]+)\1{2,}")    # /a/a/a/ loops
LONG_DIGITS_RE      = re.compile(r"\d{6,}")            # very long numeric ids
YEAR_RE             = re.compile(r"/(19|20)\d{2}(/(0?[1-9]|1[0-2]))?/")
PAGE_NUM_RE         = re.compile(r"(?:^|[?&])(page|paged|p|start|offset)=\d{3,}(?:&|$)", re.I)

MAX_URL_LEN    = 2000
MAX_QUERY_LEN  = 300
MAX_SEGMENTS   = 30


def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]


def extract_next_links(url, resp):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    # Implementation: extract links and defragment (remove URL fragments)
    # Return a list with the hyperlinks (as strings) scraped from resp.raw_response.content
    if not resp or resp.status != 200 or not getattr(resp, "raw_response", None):
        return []

    raw = resp.raw_response
    content = getattr(raw, "content", None)
    if content is None:
        return []

    # try:
    #     ctype = raw.headers.get("Content-Type", "").lower()
    #     if ctype and ("text/html" not in ctype and "application/xhtml+xml" not in ctype):
    #         return []
    # except Exception:
    #     pass

    # record analytics for this page (never break crawling)
    try:
        record_page(resp.url or url, content)
    except Exception:
        pass

    base_url = getattr(raw, "url", None) or resp.url or url

    soup = BeautifulSoup(content, "html.parser")
    out, seen = [], set()

    for tag in soup.find_all("a", href=True):
        href = tag.get("href")
        if not href:
            continue
        absolute = urljoin(base_url, href.strip())
        defragged, _ = urldefrag(absolute)
        cleaned = _canonicalize(defragged)
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            out.append(cleaned)

    return out


def is_valid(url):
    # Decide whether to crawl this url or not.
    try:
        if not url or len(url) > MAX_URL_LEN:
            return False

        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            return False

        if len(parsed.query) > MAX_QUERY_LEN:
            return False

        hostname = (parsed.hostname or "").lower()
        if not hostname:
            return False

        # --------- SCOPE: only these four domains (and their subdomains) ---------
        allowed_roots = {"ics.uci.edu", "cs.uci.edu", "informatics.uci.edu", "stat.uci.edu"}
        in_scope = any(hostname == root or hostname.endswith("." + root) for root in allowed_roots)
        if not in_scope:
            return False

        low_path  = (parsed.path or "").lower()
        low_query = (parsed.query or "").lower()
        qsl = parse_qsl(parsed.query, keep_blank_values=True)
        if len(qsl) > 12 or sum(1 for _ in qsl) > 20:
            return False

        # skip non-html resources by extension
        if re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            r"|png|tiff?|mid|mp2|mp3|mp4"
            r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            r"|epub|dll|cnf|tgz|sha1"
            r"|thmx|mso|arff|rtf|jar|csv"
            r"|rm|smil|wmv|swf|wma|zip|rar|gz|svg|ics|m3u8)$",
            low_path,
        ):
            return False

        # your prior “likely traps / low-value patterns”
        trap_keywords = [
            "calendar", "ical", "wp-json", "share", "replytocom", "format=xml",
            "action=", "sessionid", "sort=", "filter=", "feed=", "/feed/",
            "\n", "?C=;O=", "?C=N;O=D", "?C=M;O=A"
        ]
        if any(k in low_path or k in low_query for k in trap_keywords):
            return False

        # avoid repeated directory loops like /a/a/a/ or long repeated segments
        segments = [s for s in low_path.split("/") if s]
        if len(segments) > MAX_SEGMENTS:
            return False
        if any(segments.count(seg) > 3 for seg in set(segments)):
            return False

        # ---------- EXTRA protection (additions) ----------
        # common trap substrings (path/query)
        if any(s in low_path for s in TRAP_SUBSTRINGS_PATH):
            return False
        if any(s in low_query for s in TRAP_SUBSTRINGS_QUERY):
            return False

        # repeated segments pattern
        if REPEATED_SEGMENT_RE.search(low_path):
            return False

        # extremely long numeric runs in path
        if LONG_DIGITS_RE.search(low_path):
            return False

        # archives/calendars combined with year-like segments
        if YEAR_RE.search(low_path) and ("events" in low_path or "archive" in low_path or "calendar" in low_path):
            return False

        # runaway pagination (?page=999, ?offset=1000, etc.)
        if PAGE_NUM_RE.search(low_query):
            return False

        return True
    except Exception:
        return False


def _canonicalize(u: str) -> str:
    """
    Remove blacklisted params, collapse //, normalize netloc case, and rebuild without fragments.
    """
    try:
        p = urlparse(u)
        if p.scheme not in {"http", "https"}:
            return ""

        # drop noisy params (tracking/sessions/etc.)
        kept_q = [(k, v) for (k, v) in parse_qsl(p.query, keep_blank_values=False)
                  if k.lower() not in QUERY_PARAM_BLACKLIST]
        q = urlencode(kept_q, doseq=True)

        # collapse multiple slashes in path
        path = re.sub(r"/{2,}", "/", p.path or "/")

        # normalize host case
        netloc = (p.netloc or "").lower().strip()

        # rebuild (fragment already removed earlier via urldefrag)
        return urlunparse((p.scheme, netloc, path, "", q, ""))
    except Exception:
        return ""

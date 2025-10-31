import re
from urllib.parse import urlparse, urljoin, urldefrag
from bs4 import BeautifulSoup
from utils.analytics import record_page


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

	# record analytics for this page
	try:
		record_page(resp.url or url, content)
	except Exception:
		# analytics should never break crawling
		pass

	base_url = getattr(raw, "url", None) or resp.url or url

	links = []

	# Parse the HTML content using BeautifulSoup
	soup = BeautifulSoup(content, "html.parser")
	# Iterate over all anchor tags ('a') that have an 'href' attribute
	for tag in soup.find_all("a", href=True):
		href = tag.get("href")
		if not href:
			continue
		absolute = urljoin(base_url, href.strip())
		defragged, _ = urldefrag(absolute)
		links.append(defragged)

	# Deduplicate while preserving order
	seen = set()
	unique_links = []
	for link in links:
		if link not in seen:
			seen.add(link)
			unique_links.append(link)

	return unique_links


def is_valid(url):
	# Decide whether to crawl this url or not. 
	# If you decide to crawl it, return True; otherwise return False.
	# There are already some conditions that return False.
	try:
		parsed = urlparse(url)
		if parsed.scheme not in set(["http", "https"]):
			return False

		# quick length-based trap guards
		if len(url) > 2000:
			return False
		if len(parsed.query) > 300:
			return False

		# check if the hostname is in the allowed domains
		hostname = parsed.hostname
		if not hostname:
			return False

		allowed_domains = {
			"ics.uci.edu",
			"cs.uci.edu",
			"informatics.uci.edu",
			"stat.uci.edu",
		}

		hostname = hostname.lower()
		in_scope = any(
			hostname == domain or hostname.endswith("." + domain)
			for domain in allowed_domains
		)
		if not in_scope:
			return False

		# disallow likely traps / low-value patterns
		trap_keywords = [
			"calendar", "ical", "wp-json", "share", "replytocom", "format=xml",
			"action=", "sessionid", "sort=", "filter=", "feed=", "/feed/",
			"\n", "?C=;O=", "?C=N;O=D", "?C=M;O=A"
		]
		low_path = (parsed.path or "").lower()
		low_query = (parsed.query or "").lower()
		if any(k in low_path or k in low_query for k in trap_keywords):
			return False

		# avoid repeated directory loops like /a/a/a/ or long repeated segments
		segments = [s for s in low_path.split("/") if s]
		if len(segments) > 30:
			return False
		if any(segments.count(seg) > 3 for seg in set(segments)):
			return False

		# skip non-html resources by extension
		if re.match(
				 r".*\.(css|js|bmp|gif|jpe?g|ico"
				 + r"|png|tiff?|mid|mp2|mp3|mp4"
				 + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
				 + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
				 + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
				 + r"|epub|dll|cnf|tgz|sha1"
				 + r"|thmx|mso|arff|rtf|jar|csv"
				 + r"|rm|smil|wmv|swf|wma|zip|rar|gz|svg|ics|m3u8)$",
				 parsed.path.lower(),
			):
			return False

		return True

	except TypeError:
		print ("TypeError for ", parsed)
		raise


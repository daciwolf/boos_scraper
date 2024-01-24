import requests
import re
from pyisemail import is_email
from email_validator import validate_email
from bs4 import BeautifulSoup
from googlesearch import search
from urllib.parse import urlsplit, urljoin
import concurrent.futures
import multiprocessing
from pathlib import Path
from timeoutpool import TimeoutPool



class BoosScraper:
    def __init__(self, timeout_duration=10, results=1, names=Path, emails=Path):
        self.timeout_duration = timeout_duration
        self.results = results
        self.links = names
        self.emails = emails
        pass


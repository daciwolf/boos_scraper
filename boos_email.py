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



class boos_email_collector:
    
    def __init__(self, timeout_duration = 10, results=1, links = Path, emails = Path):
        self.timeout_duration= timeout_duration
        self.results = results
        self.links = links
        self.emails = emails
        pass
    
    def save_to_file(self, string: str, file: Path)-> None:
        open_file = file.open('a')
        open_file.write(string+ '\n')
        open_file.close() 

    def save_list_to_file(self, list:[], file:Path)-> None:
        open_file = file.open('a')
        for string in list:
            open_file.write(string+ '\n')
        open_file.close() 
    
    def get_response(self, url: str)-> requests.get:
        try:
            response = requests.get(url, timeout= self.timeout_duration)
            print("response recived for "+ url)
            return response
            
        except:
            print('timeout')
            pass
    
    def find_emails(self, website_text:str)-> []:
        emails = list(set(re.findall(r'[A-Za-z0-9\.\_%+-]+@[A-Za-z0-9\.\_%+-]+[A-Za-z0-9\.\_%+-]', website_text,re.I)))
        print(emails)
        return emails
    
    def email_validator(self, email:str) -> bool:
        if is_email(email, check_dns = True):
            try:
                emailinfo= validate_email(email, check_deliverability = True)
                return True
            except:
                return False
        else:
            return False
        
    def perform_search(self, query:str)-> []:
        return list(search(query, num=self.results, stop=self.results,pause=.5))
    
    def get_connecting(self, url:str) -> []:
        url_web = set()
        url_web.add(url)
        url_parts = urlsplit(url)
        try:
            website = self.get_response(url)
            soup = BeautifulSoup(website.text, 'html.parser')
            urls = soup.find_all('a')
            for url in urls:
                href = url.get('href')
                if href and href.startswith('http'):
                    url_web.add(href)
                else:
                    url_web.add(f'{url_parts.scheme}://{url_parts.hostname}/{href}')
            print('got connecting for ' + url)
        except:
            pass
        return list(url_web)
    
    
    def get_all_connecting(self, urls: []) ->[]:
        all_urls = set(urls)
        for url in urls:
            all_urls.append(self.get_connecting(url))
        return all_urls
    
    def get_urls(self) -> []:
        urls = self.links.open('r')
        urls_list = urls.readlines()
        urls.close()
        urls_list = [url.strip() for url in urls_list]
        return urls_list

    def segment_list(self, links:[])-> []:
        return [links[i:i+16] for i in range(0, len(links), 16)]
    
    def split_links(self, links: []) -> []:
        split_links = []
        for i in range(multiprocessing.cpu_count()*2):
            split_links.append(links[i::multiprocessing.cpu_count()*2])
        return split_links
    
    def threaded_get_response(self, urls:[])-> []:
        with concurrent.futures.ThreadPoolExecutor() as pool:
            responses = list(pool.map(self.get_response, urls))
        return responses
    
    def threaded_get_connecting(self, urls:[])-> []:
        with concurrent.futures.ThreadPoolExecutor() as pool:
            all_connecting = pool.map(self.get_connecting, urls)
        return list(all_connecting)
    
    def flatten_list(self, lst:[]):
        result = []
        for element in lst:
            if type(element) is list:
                result.extend(self.flatten_list(element))
            else:
                result.append(element)
        return result

    def get_text(self, websites:[])-> []:
        websites_text = []
        for website in websites:
            try:
                websites_text.append(website.text)
            except:
                pass
        return websites_text

    def email_adder(self, email:str) -> None:
        if self.email_validator(email):
            print('adding ' + email)
            self.save_to_file(email, self.emails)
    
    def process_links(self, urls:[]) -> set():
        emails = set()
        all_urls = set()
        for url in self.segment_list(urls):
            split_links = self.split_links(url) #returns a list of lists with each inner list containing a an equal split of links
            print(split_links)
            with concurrent.futures.ProcessPoolExecutor(max_workers= multiprocessing.cpu_count()) as executor:
                resulting_web = executor.map(self.threaded_get_connecting, split_links) # takes in a the list of lists and for each one creates a threaded section were it gets all the connecting links and adds them all to one big list
            print('got resulting web')
            resulting_web = self.flatten_list(list(resulting_web))
            print(resulting_web)
            resulting_web = [value for value in resulting_web if value is not None]
            split_resulting_web = self.split_links(resulting_web)
            with concurrent.futures.ProcessPoolExecutor(max_workers= multiprocessing.cpu_count()) as executor:
                all_results = list(executor.map(self.threaded_get_response, split_resulting_web))
            print('got all responses')
            all_results = self.flatten_list(all_results)
            all_results = [value for value in all_results if value is not None]
            tpool = TimeoutPool(n_jobs=multiprocessing.cpu_count(), timeout=10)
            all_emails = tpool.apply(self.find_emails, self.get_text(all_results))
            print('got emails')
            all_emails = self.flatten_list(all_emails)
            all_emails = list(set([value for value in all_emails if value is not None]))
            print('got flatened list')
            self.save_list_to_file(all_emails, self.emails)

        
    

    

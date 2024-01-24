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
    def __init__(self, timeout_duration=10, results=1, names=Path, emails=Path, school_dicts=Path):
        self.timeout_duration = timeout_duration
        self.results = results
        self.names = names
        self.emails = emails
        self.school_dicts_file = school_dicts
        pass

    def get_names(self, names: Path) -> []:
        names_text = names.open('r')
        names_list = names_text.readlines()
        names_text.close()
        return names_list

    def school_dicts(self, names: []) -> []:
        return_school_dicts = []
        for school in names:
            school_dict = {'name': school, 'links ': [], 'emails': []}
            return_school_dicts.append(school_dict)
        return return_school_dicts

    def perform_search(self, query: str) -> []:
        return list(search(query, num=self.results, stop=self.results, pause=.5))

    def save_list_to_file(self, list: [], file: Path) -> None:
        open_file = file.open('a')
        for string in list:
            open_file.write(string + '\n')
        open_file.close()

    def segment_list(self, schools: []) -> []:
        return [schools[i:i + 16] for i in range(0, len(schools), multiprocessing.cpu_count() * 2)]

    def split_links(self, links: []) -> []:
        split_links = []
        for i in range(multiprocessing.cpu_count() * 2):
            split_links.append(links[i::multiprocessing.cpu_count() * 2])
        return split_links

    def get_connecting(self, school: dict) -> dict:
        url = school['links'][0]
        url_web = set(url)
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
        school['links'] = list(url_web)
        return school

    def threaded_get_connecting(self, schools: []) -> []:
        with concurrent.futures.ThreadPoolExecutor() as pool:
            schools_with_web = pool.map(self.get_connecting, schools)
        return list(schools_with_web)

    def get_response(self, url: str) -> requests.get:
        try:
            response = requests.get(url, timeout=self.timeout_duration)
            print("response recived for " + url)
            return response

        except:
            print('timeout')
            pass


    def threaded_get_response(self, school_with_web:dict)-> dict:
        with concurrent.futures.ThreadPoolExecutor() as pool:
            responses = list(pool.map(self.get_response, school_with_web['links']))
            school_with_web['responses'] = responses
        return school_with_web

    def get_text(self, websites:[])-> []:
        websites_text = []
        for website in websites:
            try:
                websites_text.append(website.text)
            except:
                pass
        return websites_text

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

    def school_find_emails(self, school:dict)-> dict:
        for response in school['responses']:
            try:
                email = self.find_emails(response.text)
                if self.email_validator(email):
                    school['emails'].append(email)
            except:
                pass


    def school_text(self, school:dict)-> str:
        return 'name: ' + school['name'] + '\nemail: ' + school['emails']




    def proccess_names(self, names: Path, emails: Path):
        school_dicts = self.school_dicts(self.get_names(names))
        for school in school_dicts:
            try:
                school['links'].append(self.perform_search(school['name'])[0])
            except Exception as e:
                pass
        self.save_list_to_file(school_dicts, self.school_dicts_file)
        for schools in self.segment_list(school_dicts):
            split_schools = self.split_links(schools)
            print(split_schools)
            with concurrent.futures.ProcessPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
                schools_with_web = list(executor.map(self.threaded_get_connecting, split_schools))
            print('got web for schools')
            with concurrent.futures.ProcessPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
                schools_with_responses = list(executor.map(self.threaded_get_response, schools_with_web))
            print('got schools with results')
            tpool = TimeoutPool(n_jobs=multiprocessing.cpu_count(), timeout=10)
            schools_with_emails = tpool.apply(self.school_find_emails, schools_with_responses)
            print('got emails')
            schools_text = []
            for school in schools_with_emails:
                schools_text.append(self.school_text(school))
            self.save_list_to_file(schools_text, self.emails)


if __name__ == '__main__':
    names_path = Path(r'C:\Users\david\PycharmProjects\boos_scraper\names.txt')
    emails_path = Path(r'C:\Users\david\PycharmProjects\boos_scraper\emails.txt')
    school_dicts_path = Path(r'C:\Users\david\PycharmProjects\boos_scraper\school_dicts.txt')
    scraper = BoosScraper(names=names_path, emails=emails_path, school_dicts=school_dicts_path)
    scraper.proccess_names(names=scraper.names, emails=scraper.emails )



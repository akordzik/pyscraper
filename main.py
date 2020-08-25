import os
import requests
import pymongo
import hashlib
import smtplib
import json

from apscheduler.schedulers.blocking import BlockingScheduler
from pymongo.mongo_client import MongoClient
from pymongo.results import UpdateResult
from pymongo.collection import Collection
from typing import List, Tuple
from datetime import datetime
from requests.models import Response


class Advertisement:
    def __init__(self, adv):
        self.adv = adv
        self.adv_details = adv.find('div', class_='offer-item-details')

    def title(self) -> str:
        title = self.adv.find('span', class_='offer-item-title')
        return title.text

    def link(self) -> str:
        header = self.adv.find('header', class_='offer-item-header')
        link = header.find('a')
        return str.split(link.attrs['href'], '#')[0]

    def rooms(self) -> int:
        li = self.adv_details.find('li', class_='offer-item-rooms')
        return int(li.text[0])

    def price(self) -> float:
        li = self.adv_details.find('li', class_='offer-item-price')
        return float(list(li.stripped_strings)[0][:-3].replace(' ', '').replace(',', '.'))

    def price_per_m(self) -> float:
        li = self.adv_details.find('li', class_='offer-item-price-per-m')
        return float(li.text[:-6].replace(' ', ''))

    def area(self) -> float:
        li = self.adv_details.find('li', class_='offer-item-area')
        return float(li.text[:-3].replace(',', '.'))

    def key(self) -> int:
        return int(hashlib.sha256(Advertisement._extract_key(self.link()).encode('utf-8')).hexdigest(), 16) % 10**8

    @staticmethod
    def _extract_key(link: str) -> str:
        last_dash = link.rindex('-')
        last_dot = link.rindex('.')
        return link[last_dash + 1:last_dot]


with open('config.json', 'r') as config:
    configuration = json.loads(config.read())
    urls: List[str] = configuration['urls']


def get_collection(mongo_uri: str) -> Tuple[Collection, MongoClient]:
    client = pymongo.MongoClient(mongo_uri)
    db = client.get_default_database()
    collection = db.get_collection('advertisements')

    return collection, client


def try_upsert(collection: Collection, advertisement: Advertisement) -> UpdateResult:
    return collection.update_one({'_id': advertisement.key()}, [
        {
            '$replaceRoot': {
                'newRoot': {
                    '$mergeObjects': [
                        '$$ROOT', {
                            'price_historical': {
                                '$ifNull': [
                                    {
                                        '$concatArrays': [
                                            '$$ROOT.price_historical',
                                                {
                                                    '$cond': {'if': {'$eq': ['$$ROOT.price', advertisement.price()]}, 'then': [], 'else': [
                                                        {
                                                            'price': advertisement.price(),
                                                            'updated_at': '$$NOW'
                                                        }
                                                    ]}
                                                }
                                        ]
                                    },
                                    [
                                        {
                                            'price': advertisement.price(),
                                            'updated_at': '$$NOW'
                                        }
                                    ]
                                ]
                            },
                            'title': advertisement.title(),
                            'link': advertisement.link(),
                            'rooms': advertisement.rooms(),
                            'area': advertisement.area(),
                            'added_at': {
                                '$ifNull': ['$$ROOT.added_at', '$$NOW']
                            }
                        }
                    ]
                }
            }
        }, {
            '$set': {
                'price_per_m': advertisement.price_per_m(),
                'price': advertisement.price()
            }
        }, {
            '$unset': 'expired_at'
        }
    ], upsert=True)


def try_mark_outdated(scraped_ids: list, collection: Collection) -> List[dict]:
    find = {'$and': [
        {'_id': {'$not': {'$in': scraped_ids}}},
        {'expired_at': {'$exists': False}}
    ]}
    to_be_updated = list(collection.find(find))

    if len(to_be_updated) > 0:
        collection.update_many(find, {
            '$set': {'expired_at': datetime.now()}
        })

    return to_be_updated


def generate_html(upserted_adverts: List[Advertisement], modified_adverts: List[Advertisement], outdated_adverts: List[dict]) -> str:
    from jinja2 import Environment, FileSystemLoader

    env = Environment(loader=FileSystemLoader('./static/'))
    template = env.get_template('template.html')
    template_vars = {
        'upserted_adverts': upserted_adverts,
        'modified_adverts': modified_adverts,
        'outdated_adverts': outdated_adverts,
    }
    html_out = template.render(template_vars)

    return html_out


def send_mail(upserted_adverts: List[Advertisement], modified_adverts: List[Advertisement], outdated_adverts: List[dict]):
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    if len(upserted_adverts) + len(modified_adverts) + len(outdated_adverts) == 0:
        return

    sender = 'pyscraper <py@scraper.com>'
    receiver = os.environ['SMTP_RECEIVER']

    message = MIMEMultipart('alternative')
    message['Subject'] = 'Scraping results'
    message['From'] = sender
    message['To'] = receiver

    html = generate_html(upserted_adverts, modified_adverts, outdated_adverts)

    message.attach(MIMEText(html, 'html'))

    with smtplib.SMTP(os.environ['SMTP_HOST'], os.environ['SMTP_PORT']) as server:
        server.login(os.environ['SMTP_LOGIN'], os.environ['SMTP_PASSWORD'])
        server.sendmail(sender, receiver, message.as_string())


def extract_adverts(page_content: Response) -> List[Advertisement]:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(page_content.content, 'html.parser')
    container = soup.find(id='body-container')
    articles = container.find_all('article')
    adverts = list(map(lambda x: Advertisement(x), articles[3:]))

    return adverts


def main():
    print('Started scraping')

    try:
        collection, client = get_collection(os.environ['MONGOLAB_URI'])
        upserted_adverts: List[Advertisement] = []
        modified_adverts: List[Advertisement] = []
        scraped_ids = []

        for url in urls:
            page = 1

            while True:
                url_page = f'&page={page}' if page > 1 else ''
                page_content = requests.get(
                    url + url_page, allow_redirects=False)

                if page_content.status_code > 299:
                    break

                adverts = extract_adverts(page_content)

                for advertisement in adverts:
                    result = try_upsert(collection, advertisement)

                    if result.modified_count > 0:
                        modified_adverts.append(advertisement)
                    elif result.upserted_id != None:
                        upserted_adverts.append(advertisement)

                    scraped_ids.append(advertisement.key())

                page = page + 1

        outdated_adverts = try_mark_outdated(scraped_ids, collection)

        send_mail(upserted_adverts, modified_adverts, outdated_adverts)

        for a in modified_adverts:
            print(f'Modified: {a.title()}')

        for a in upserted_adverts:
            print(f'Upserted: {a.title()}')

        for a in outdated_adverts:
            print(f'Outdated: {a["title"]}')
    finally:
        print('Finished scraping')
        client.close()


minutes = int(os.environ['MINUTES'])
sched = BlockingScheduler()


@sched.scheduled_job('interval', minutes=minutes)
def timed_job():
    main()


sched.start()

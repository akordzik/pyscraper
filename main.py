import os
import requests
import pymongo
import hashlib
import smtplib

from apscheduler.schedulers.blocking import BlockingScheduler
from pymongo.results import UpdateResult
from pymongo.collection import Collection
from bs4 import BeautifulSoup
from typing import List
from datetime import datetime


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


def generate_html(upserted_adverts: List[Advertisement]) -> str:
    from jinja2 import Environment, FileSystemLoader

    env = Environment(loader=FileSystemLoader('./static/'))
    template = env.get_template("template.html")
    template_vars = {
        "upserted_adverts": upserted_adverts,
    }
    html_out = template.render(template_vars)

    return html_out


def send_mail(upserted_adverts: List[Advertisement], modified_adverts: List[Advertisement], to_be_updated: List[dict]):
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    if len(upserted_adverts) + len(modified_adverts) + len(to_be_updated) == 0:
        return

    sender = "pyscraper <py@scraper.com>"
    receiver = os.environ['SMTP_RECEIVER']

    message = MIMEMultipart("alternative")
    message["Subject"] = "Scraping results"
    message["From"] = sender
    message["To"] = receiver

    html = generate_html(upserted_adverts)

    message.attach(MIMEText(html, 'html'))

    with smtplib.SMTP(os.environ['SMTP_HOST'], os.environ['SMTP_PORT']) as server:
        server.login(os.environ['SMTP_LOGIN'], os.environ['SMTP_PASSWORD'])
        server.sendmail(sender, receiver, message.as_string())


def main():
    client = pymongo.MongoClient(os.environ['MONGOLAB_URI'])
    db = client.get_default_database()
    collection = db.get_collection('advertisements')
    urls = [
        'https://www.otodom.pl/sprzedaz/mieszkanie/?search%5Bfilter_float_price%3Ato%5D=800000&search%5Bfilter_float_price_per_m%3Ato%5D=12000&search%5Bfilter_float_m%3Afrom%5D=40&search%5Bfilter_float_m%3Ato%5D=70&search%5Bfilter_enum_floor_no%5D%5B0%5D=floor_1&search%5Bfilter_enum_floor_no%5D%5B1%5D=floor_2&search%5Bfilter_enum_floor_no%5D%5B2%5D=floor_3&search%5Bfilter_enum_floor_no%5D%5B3%5D=floor_4&search%5Bfilter_enum_floor_no%5D%5B4%5D=floor_5&search%5Bfilter_enum_floor_no%5D%5B5%5D=floor_6&search%5Bfilter_enum_floor_no%5D%5B6%5D=floor_7&search%5Bfilter_enum_floor_no%5D%5B7%5D=floor_8&search%5Bfilter_enum_floor_no%5D%5B8%5D=floor_9&search%5Bfilter_enum_floor_no%5D%5B9%5D=floor_10&search%5Bfilter_enum_floor_no%5D%5B10%5D=floor_higher_10&search%5Bfilter_enum_floor_no%5D%5B11%5D=garret&search%5Bdescription%5D=1&search%5Bprivate_business%5D=private&search%5Border%5D=created_at_first%3Adesc&locations%5B0%5D%5Bregion_id%5D=7&locations%5B0%5D%5Bsubregion_id%5D=197&locations%5B0%5D%5Bcity_id%5D=26&locations%5B0%5D%5Bdistrict_id%5D=42&locations%5B1%5D%5Bregion_id%5D=7&locations%5B1%5D%5Bsubregion_id%5D=197&locations%5B1%5D%5Bcity_id%5D=26&locations%5B1%5D%5Bdistrict_id%5D=847&locations%5B2%5D%5Bregion_id%5D=7&locations%5B2%5D%5Bsubregion_id%5D=197&locations%5B2%5D%5Bcity_id%5D=26&locations%5B2%5D%5Bdistrict_id%5D=39&locations%5B3%5D%5Bregion_id%5D=7&locations%5B3%5D%5Bsubregion_id%5D=197&locations%5B3%5D%5Bcity_id%5D=26&locations%5B3%5D%5Bdistrict_id%5D=44&locations%5B4%5D%5Bregion_id%5D=7&locations%5B4%5D%5Bsubregion_id%5D=197&locations%5B4%5D%5Bcity_id%5D=26&locations%5B4%5D%5Bdistrict_id%5D=724&locations%5B5%5D%5Bregion_id%5D=7&locations%5B5%5D%5Bsubregion_id%5D=197&locations%5B5%5D%5Bcity_id%5D=26&locations%5B5%5D%5Bdistrict_id%5D=40&locations%5B6%5D%5Bregion_id%5D=7&locations%5B6%5D%5Bsubregion_id%5D=197&locations%5B6%5D%5Bcity_id%5D=26&locations%5B6%5D%5Bdistrict_id%5D=53&locations%5B7%5D%5Bregion_id%5D=7&locations%5B7%5D%5Bcity_id%5D=26&locations%5B7%5D%5Bdistrict_id%5D=3319&locations%5B8%5D%5Bregion_id%5D=7&locations%5B8%5D%5Bcity_id%5D=26&locations%5B8%5D%5Bdistrict_id%5D=38&nrAdsPerPage=72',
        'https://www.otodom.pl/sprzedaz/mieszkanie/?search%5Bfilter_float_price%3Ato%5D=700000&search%5Bfilter_float_price_per_m%3Ato%5D=10000&search%5Bfilter_float_m%3Afrom%5D=40&search%5Bfilter_float_m%3Ato%5D=70&search%5Bfilter_enum_floor_no%5D%5B0%5D=floor_1&search%5Bfilter_enum_floor_no%5D%5B1%5D=floor_2&search%5Bfilter_enum_floor_no%5D%5B2%5D=floor_3&search%5Bfilter_enum_floor_no%5D%5B3%5D=floor_4&search%5Bfilter_enum_floor_no%5D%5B4%5D=floor_5&search%5Bfilter_enum_floor_no%5D%5B5%5D=floor_6&search%5Bfilter_enum_floor_no%5D%5B6%5D=floor_7&search%5Bfilter_enum_floor_no%5D%5B7%5D=floor_8&search%5Bfilter_enum_floor_no%5D%5B8%5D=floor_9&search%5Bfilter_enum_floor_no%5D%5B9%5D=floor_10&search%5Bfilter_enum_floor_no%5D%5B10%5D=floor_higher_10&search%5Bfilter_enum_floor_no%5D%5B11%5D=garret&search%5Bdescription%5D=1&search%5Bprivate_business%5D=private&search%5Border%5D=created_at_first%3Adesc&locations%5B0%5D%5Bregion_id%5D=7&locations%5B0%5D%5Bsubregion_id%5D=197&locations%5B0%5D%5Bcity_id%5D=26&locations%5B0%5D%5Bdistrict_id%5D=41&locations%5B1%5D%5Bregion_id%5D=7&locations%5B1%5D%5Bsubregion_id%5D=197&locations%5B1%5D%5Bcity_id%5D=26&locations%5B1%5D%5Bdistrict_id%5D=42&locations%5B2%5D%5Bregion_id%5D=7&locations%5B2%5D%5Bsubregion_id%5D=197&locations%5B2%5D%5Bcity_id%5D=26&locations%5B2%5D%5Bdistrict_id%5D=847&locations%5B3%5D%5Bregion_id%5D=7&locations%5B3%5D%5Bsubregion_id%5D=197&locations%5B3%5D%5Bcity_id%5D=26&locations%5B3%5D%5Bdistrict_id%5D=39&locations%5B4%5D%5Bregion_id%5D=7&locations%5B4%5D%5Bsubregion_id%5D=197&locations%5B4%5D%5Bcity_id%5D=26&locations%5B4%5D%5Bdistrict_id%5D=47&locations%5B5%5D%5Bregion_id%5D=7&locations%5B5%5D%5Bsubregion_id%5D=197&locations%5B5%5D%5Bcity_id%5D=26&locations%5B5%5D%5Bdistrict_id%5D=117&nrAdsPerPage=72'
    ]

    print('Started scraping')

    try:
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

                soup = BeautifulSoup(page_content.content, 'html.parser')
                container = soup.find(id='body-container')
                articles = container.find_all('article')
                adverts = list(map(lambda x: Advertisement(x), articles[3:]))

                for advertisement in adverts:
                    result = try_upsert(collection, advertisement)

                    if result.modified_count > 0:
                        modified_adverts.append(advertisement)
                    elif result.upserted_id != None:
                        upserted_adverts.append(advertisement)

                    scraped_ids.append(advertisement.key())

                page = page + 1

        outdated_adverts = try_mark_outdated(scraped_ids, collection)

        # send_mail(upserted_adverts, modified_adverts, outdated_adverts)

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

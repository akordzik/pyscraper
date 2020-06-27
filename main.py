import os
import requests
import pymongo
import hashlib

from apscheduler.schedulers.blocking import BlockingScheduler
from pymongo.results import UpdateResult
from pymongo.collection import Collection
from bs4 import BeautifulSoup
from decimal import Decimal
from pymongo.database import Database
from typing import List


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
        return link.attrs['href']

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
        return int(hashlib.sha256(self.link().encode('utf-8')).hexdigest(), 16) % 10**8

    def to_dict(self) -> dict:
        return {
            'key': self.key(),
            'area': self.area(),
            'price_per_m': self.price_per_m(),
            'price': self.price(),
            'rooms': self.rooms(),
            'link': self.link(),
            'title': self.title(),
        }


def try_upsert(collection: Collection, advertisement: Advertisement) -> UpdateResult:
    return collection.update_one({'_id': advertisement.key()}, [
        {
            "$replaceRoot": {
                "newRoot": {
                    "$mergeObjects": [
                        "$$ROOT", {
                            "price_historical": {
                                "$ifNull": [
                                    {
                                        "$concatArrays": [
                                            "$$ROOT.price_historical",
                                                {
                                                    "$cond": {"if": {"$eq": ["$$ROOT.price", advertisement.price()]}, "then": [], "else": [
                                                        {
                                                            "price": advertisement.price(),
                                                            "updated_at": "$$NOW"
                                                        }
                                                    ]}
                                                }
                                        ]
                                    },
                                    [
                                        {
                                            "price": advertisement.price(),
                                            "updated_at": "$$NOW"
                                        }
                                    ]
                                ]
                            },
                            "title": advertisement.title(),
                            "link": advertisement.link(),
                            "rooms": advertisement.rooms(),
                            "area": advertisement.area(),
                            "added_at": {
                                "$ifNull": ["$$ROOT.added_at", "$$NOW"]
                            }
                        }
                    ]
                }
            }
        }, {
            "$set": {
                "price_per_m": advertisement.price_per_m(),
                "price": advertisement.price()
            }
        }
    ], upsert=True)


def main():
    client = pymongo.MongoClient(os.environ['MONGOLAB_URI'])
    db: Database = client.get_default_database()
    collection = db.get_collection('advertisements')
    url = 'https://www.otodom.pl/sprzedaz/mieszkanie/?search%5Bfilter_float_price%3Ato%5D=800000&search%5Bfilter_float_price_per_m%3Ato%5D=12000&search%5Bfilter_float_m%3Afrom%5D=40&search%5Bfilter_float_m%3Ato%5D=70&search%5Bfilter_enum_floor_no%5D%5B0%5D=floor_1&search%5Bfilter_enum_floor_no%5D%5B1%5D=floor_2&search%5Bfilter_enum_floor_no%5D%5B2%5D=floor_3&search%5Bfilter_enum_floor_no%5D%5B3%5D=floor_4&search%5Bfilter_enum_floor_no%5D%5B4%5D=floor_5&search%5Bfilter_enum_floor_no%5D%5B5%5D=floor_6&search%5Bfilter_enum_floor_no%5D%5B6%5D=floor_7&search%5Bfilter_enum_floor_no%5D%5B7%5D=floor_8&search%5Bfilter_enum_floor_no%5D%5B8%5D=floor_9&search%5Bfilter_enum_floor_no%5D%5B9%5D=floor_10&search%5Bfilter_enum_floor_no%5D%5B10%5D=floor_higher_10&search%5Bfilter_enum_floor_no%5D%5B11%5D=garret&search%5Bdescription%5D=1&search%5Bprivate_business%5D=private&search%5Border%5D=created_at_first%3Adesc&locations%5B0%5D%5Bregion_id%5D=7&locations%5B0%5D%5Bsubregion_id%5D=197&locations%5B0%5D%5Bcity_id%5D=26&locations%5B0%5D%5Bdistrict_id%5D=42&locations%5B1%5D%5Bregion_id%5D=7&locations%5B1%5D%5Bsubregion_id%5D=197&locations%5B1%5D%5Bcity_id%5D=26&locations%5B1%5D%5Bdistrict_id%5D=847&locations%5B2%5D%5Bregion_id%5D=7&locations%5B2%5D%5Bsubregion_id%5D=197&locations%5B2%5D%5Bcity_id%5D=26&locations%5B2%5D%5Bdistrict_id%5D=39&locations%5B3%5D%5Bregion_id%5D=7&locations%5B3%5D%5Bsubregion_id%5D=197&locations%5B3%5D%5Bcity_id%5D=26&locations%5B3%5D%5Bdistrict_id%5D=44&locations%5B4%5D%5Bregion_id%5D=7&locations%5B4%5D%5Bsubregion_id%5D=197&locations%5B4%5D%5Bcity_id%5D=26&locations%5B4%5D%5Bdistrict_id%5D=724&locations%5B5%5D%5Bregion_id%5D=7&locations%5B5%5D%5Bsubregion_id%5D=197&locations%5B5%5D%5Bcity_id%5D=26&locations%5B5%5D%5Bdistrict_id%5D=40&locations%5B6%5D%5Bregion_id%5D=7&locations%5B6%5D%5Bsubregion_id%5D=197&locations%5B6%5D%5Bcity_id%5D=26&locations%5B6%5D%5Bdistrict_id%5D=53&locations%5B7%5D%5Bregion_id%5D=7&locations%5B7%5D%5Bcity_id%5D=26&locations%5B7%5D%5Bdistrict_id%5D=3319&locations%5B8%5D%5Bregion_id%5D=7&locations%5B8%5D%5Bcity_id%5D=26&locations%5B8%5D%5Bdistrict_id%5D=38&nrAdsPerPage=72'

    print('Started scraping')
    try:
        upserted_adverts = []
        modified_adverts: List[Advertisement] = []
        page = 1

        while True:
            url_page = f'&page={page}' if page > 1 else ''
            page_content = requests.get(url + url_page, allow_redirects=False)

            if page_content.status_code > 299:
                break

            soup = BeautifulSoup(page_content.content, 'html.parser')
            container = soup.find(id='body-container')
            articles = container.find_all('article')

            for article in articles[3:]:
                advertisement = Advertisement(article)
                result = try_upsert(collection, advertisement)

                if result.modified_count > 0:
                    modified_adverts.append(advertisement)

                if result.upserted_id != None:
                    upserted_adverts.append(advertisement)

            page = page + 1

        for a in modified_adverts:
            print(f'Modified: {a.title()}')

        for a in upserted_adverts:
            print(f'Upserted: {a.title()}')
    finally:
        print('Finished scraping')
        client.close()


minutes = int(os.environ['MINUTES'])
sched = BlockingScheduler()


@sched.scheduled_job('interval', minutes=minutes)
def timed_job():
    main()


sched.start()

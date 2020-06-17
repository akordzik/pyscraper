from apscheduler.schedulers.blocking import BlockingScheduler
import pymongo
import os

sched = BlockingScheduler()
client = pymongo.MongoClient(os.environ['MONGOLAB_URI'])


@sched.scheduled_job('interval', minutes=1)
def timed_job():
    db = client.test
    print(db)


sched.start()

# import os
# import requests
# import pymongo
# from bs4 import BeautifulSoup
# from decimal import Decimal


# class Advertisement:
#     def __init__(self, adv):
#         self.adv = adv
#         self.adv_details = adv.find('div', class_='offer-item-details')

#     def title(self) -> str:
#         title = self.adv.find('span', class_='offer-item-title')
#         return title.text

#     def link(self) -> str:
#         header = self.adv.find('header', class_='offer-item-header')
#         link = header.find('a')
#         return link.attrs['href']

#     def rooms(self) -> int:
#         li = self.adv_details.find('li', class_='offer-item-rooms')
#         return int(li.text[0])

#     def price(self) -> float:
#         li = self.adv_details.find('li', class_='offer-item-price')
#         return float(list(li.stripped_strings)[0][:-3].replace(' ', '').replace(',', '.'))

#     def price_per_m(self) -> float:
#         li = self.adv_details.find('li', class_='offer-item-price-per-m')
#         return float(li.text[:-6].replace(' ', ''))

#     def area(self) -> float:
#         li = self.adv_details.find('li', class_='offer-item-area')
#         return float(li.text[:-3].replace(',', '.'))

#     def key(self) -> int:
#         return hash(self.link())

#     def to_dict(self) -> dict:
#         return {
#             'key': self.key(),
#             'area': self.area(),
#             'price_per_m': self.price_per_m(),
#             'price': self.price(),
#             'rooms': self.rooms(),
#             'link': self.link(),
#             'title': self.title(),
#         }


# client = pymongo.MongoClient(os.environ['MONGOLAB_URI'])
# db = client.get_default_database()
# collection = db.get_collection('advertisements')

# try:
#     URL = 'https://www.otodom.pl/sprzedaz/mieszkanie/?search[filter_float_price%3Ato]=800000&search[filter_float_price_per_m%3Ato]=12000&search[filter_float_m%3Afrom]=50&search[description]=1&search[private_business]=private&search[order]=created_at_first%3Adesc&locations[0][region_id]=7&locations[0][subregion_id]=197&locations[0][city_id]=26&locations[0][district_id]=42&locations[1][region_id]=7&locations[1][subregion_id]=197&locations[1][city_id]=26&locations[1][district_id]=847&locations[2][region_id]=7&locations[2][subregion_id]=197&locations[2][city_id]=26&locations[2][district_id]=39&locations[3][region_id]=7&locations[3][subregion_id]=197&locations[3][city_id]=26&locations[3][district_id]=44&locations[4][region_id]=7&locations[4][subregion_id]=197&locations[4][city_id]=26&locations[4][district_id]=724&locations[5][region_id]=7&locations[5][subregion_id]=197&locations[5][city_id]=26&locations[5][district_id]=40&locations[6][region_id]=7&locations[6][subregion_id]=197&locations[6][city_id]=26&locations[6][district_id]=53&locations[7][region_id]=7&locations[7][city_id]=26&locations[7][district_id]=3319&locations[8][region_id]=7&locations[8][city_id]=26&locations[8][district_id]=38&nrAdsPerPage=72'
#     page = requests.get(URL)
#     soup = BeautifulSoup(page.content, 'html.parser')
#     container = soup.find(id='body-container')
#     articles = container.find_all('article')
#     adverts = []

#     for article in articles:
#         advertisement = Advertisement(article)
#         adverts.append(advertisement.to_dict())

#     result = collection.insert_many(adverts)
#     print(result)
# finally:
#     client.close()

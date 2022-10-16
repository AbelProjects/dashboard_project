import json
import requests
import datetime
from google.protobuf.json_format import MessageToJson
from proto_structs import offers_pb2
from typing import List
import pickle
from tqdm import tqdm
import numpy as np
import pandas as pd
import os


def get_url(city, shop, page_num):
    url = f"https://squark.edadeal.ru/web/search/offers?count=50&locality={city}&page={page_num}&retailer={shop}"
    return url


def check_url_code(city: str, shop: str, page_num: int):
    url = get_url(city, shop, page_num)
    return requests.get(url, allow_redirects=True).status_code


def parse_page(city: str, shop: str, page_num: int):
    """
    :param city: location of the shop
    :param shop: shop name
    :param page_num: parsed page number
    :return: None
    """

    url = get_url(city, shop, page_num)
    data = requests.get(url, allow_redirects=True)  # data.content is a protobuf message

    offers = offers_pb2.Offers()  # protobuf structure
    offers.ParseFromString(data.content)  # parse binary data
    products: str = MessageToJson(offers)  # convert protobuf message to json
    products = json.loads(products)
    data = []
    for prod in products['offer']:
        prod['city'] = city
        prod['shop'] = shop
        prod['page'] = page_num
        prod['processed_date'] = datetime.date.today().strftime('%Y-%m-%d')
        data.append(prod)
    return data


def parse_shop(city: str = 'moskva', shop: str = '5ka', page_range: int = 100, skip_errors=False):
    shop_data = []
    for page in range(page_range):
        code = check_url_code(city, shop, page_num=page)
        if code == 200:
            data = parse_page(city, shop, page_num=page)
            shop_data.extend(data)
        elif code == 204:
            if page > 1:
                print(f'No more products in {shop}')
            else:
                print(f'No data for {shop}')
            shop_data = pd.DataFrame(shop_data)
            return shop_data
        else:
            if skip_errors:
                print(f'Unexpected code {code} for {shop}')
                return shop_data
            else:
                raise ValueError(f'Unexpected code {code} for {shop}')
    shop_data = pd.DataFrame(shop_data)
    return shop_data


def parse_shop_list(city: str, shop_list: List[str], page_range=100):
    data = pd.DataFrame()
    for shop in tqdm(shop_list):
        shop_data = parse_shop(city=city, shop=shop, page_range=page_range)
        data = pd.concat([data, shop_data])
    return data


def save_results(data):
    now_date = datetime.date.today().strftime('%Y_%m_%d')
    data.to_csv(f'data/prices_{now_date}.csv', index=False)


if __name__ == '__main__':
    shop_list = ['5ka', 'magnit-univer', 'perekrestok', 'dixy',
                 'lenta-super', 'vkusvill_offline', 'mgnl', 'azbuka_vkusa']
    data = parse_shop_list(city='moskva', shop_list=shop_list, page_range=100)
    save_results(data)

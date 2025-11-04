import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получить список товаров с Ozon.
    
    Args:
        last_id (str): Идентификатор последнего значения на странице.
        client_id (str): Идентификатор клиента Ozon
        seller_token (str): API-ключ Ozon
        
    Returns:
        dict: Словарь с данными о товарах
        
    Examples:
        >>> get_product_list("", "123", "456")
        {
            "items": [
                {"offer_id": "136748"}
            ], 
            "total": 1, 
            "last_id": "bnVсbA=="
        }
        
        >>> get_product_list("", "invalid_id", "invalid_token")
        requests.exceptions.HTTPError: 401 Client Error: Unauthorized
    """
    
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получить все идентификаторы товаров с Ozon
    
    Args:
        client_id (str): Идентификатор клиента Ozon
        seller_token (str): API-ключ Ozon
        
    Returns:
        list: Список идентификаторов всех товаров Ozon
        
    Examples:
        >>> get_offer_ids("123", "456")
        ["1", "2", "3"]
        
        >>> get_offer_ids("invalid_id", "invalid_token")
        requests.exceptions.HTTPError: 401 Client Error: Unauthorized
    """
    
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновить цены товаров на Ozon.

    Args:
        prices (list): Список словарей с данными о товарах.
        client_id (str): Идентификатор клиента Ozon
        seller_token (str): API-ключ Ozon
        
    Returns:
        dict: Ответ от API Ozon с результатом обновления цен.
        
    Examples:
        >>> prices = [
            {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": "123",
                "old_price": "0",
                "price": 5990,
                }
            ]
        >>> update_price(prices, "123", "456")
        {
            "result": [
                {
                    "product_id": 1386,
                    "offer_id": "123",
                    "updated": true,
                    "errors": [ ]
                }
            ]
        }
        
        >>> update_price([], "123", "456")
        requests.exceptions.HTTPError: 400 Client Error: Bad Request
    """
    
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновить остатки товаров на Ozon.
    
    Args:
        stocks (list): Список словарей с данными об остатках товаров.
        client_id (str): Идентификатор клиента Ozon
        seller_token (str): API-ключ Ozon
        
    Returns:
        dict: Ответ от API Ozon с результатом обновления остатков.
        
    Examples:
        >>> stocks = [{"offer_id": "123", "stock": 10}]
        >>> update_stocks(stocks, "123", "456")
        {
            "result": [
                {
                    "offer_id": "123",
                    "updated": true,
                    "errors": [ ]
                }
            ]
        }
        
        >>> update_stocks([{"offer_id": "invalid", "stock": 0}], "123", "456")
        requests.exceptions.HTTPError: 400 Client Error: Bad Request
    """
    
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачать и обработать файл с остатками товаров с сайта Casio.
    
    Returns:
        list: Список словарей с данными об остатках товаров.
        
    Examples:
        >>> download_stock()
        [
            {
                "Код": "123",
                "Наименование товара": "BA-110-4A1"
                "Цена": "16'590.00 руб."
                "Количество": "5", 
            }
        ]
        
        >>> download_stock()
        requests.exceptions.HTTPError: 404 Client Error: Not Found
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Создать список остатков товаров для обновления на Ozon.
    
    Args:
        watch_remnants (list): Список словарей с данными об остатках товаров.
        offer_ids (list): Список идентификаторов товаров, присутствующих в Ozon.
        
    Returns:
        list: Список словарей с данными об остатках для Ozon.
        
    Examples:
        >>> watch_remnants = [
            {
                "Код": "123", 
                "Наименование товара": "BA-110-4A1"
                "Цена": "16'590.00 руб."
                "Количество": ">10", 
            }
        ]
        >>> offer_ids = ["123", "456"]
        >>> create_stocks(watch_remnants, offers)
        [
            {
                "offer_id": "123", 
                "stock": 100
            }, 
            {
                "offer_id": "456", 
                "stock": 0
            }
        ]
        
        >>> create_stocks([], ["123"])
        [
            {
                "offer_id": "123",
                "stock": 0
            }
        ]
    """
    
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создать список цен товаров для обновления на Ozon.
    
    Args:
        watch_remnants (list): Список словарей с данными об остатках товаров.
        offer_ids (list): Список идентификаторов товаров, присутствующих в Ozon.
        
    Returns:
        list: Список словарей с данными о ценах товаров для API Ozon.
        
    Examples:
        >>> watch_remnants = [
            {
                "Код": "123", 
                "Наименование товара": "BA-110-4A1"
                "Цена": "16'590.00 руб."
                "Количество": ">10", 
            }
        ]
        >>> offer_ids = ["123"]
        >>> create_prices(watch_remnants, offer_ids)
        [
            {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": "123",
                "old_price": "0",
                "price": "16590",
            }
        ]
        
        >>> create_prices([], ["123"])
        []
    """
    
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразовать цену.
    
    Args:
        price (str): Строка с ценой товара в исходном формате.
        
    Returns:
        str: Преобразованная цена товара без валюты и разделителей.
        
    Examples:
        >>> price_conversion("5'990.00 руб.")
        "5990"
        
        >>> price_conversion("abc")
        ""
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделить список list на части по n элементов
    
    Args:
        lst (list): Список для разделения.
        n (int): Размер каждой части.
        
    Yields:
        list: Часть списка размером n элементов.
        
    Examples:
        >>> list(divide([1, 2, 3, 4, 5], 2))
        [[1, 2], [3, 4], [5]]
        
        >>> list(divide([], 2))
        []
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Асинхронно загрузить цены товаров на Ozon.

    Args:
        watch_remnants (list): Список словарей с данными о ценах товаров.
        client_id (str): Идентификатор клиента Ozon
        seller_token (str): API-ключ Ozon
        
    Returns:
        list: Список словарей с данными о ценах товаров для API Ozon.
        
    Examples:
        >>> await upload_prices(watch_remnants, "123", "456")
        [
            {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": "123",
                "old_price": "0",
                "price": "16590",
            }
        ]
        
        >>> await upload_prices([], "invalid_client", "invalid_token")
        requests.exceptions.HTTPError: 401 Client Error: Unauthorized
    """
    
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Асинхронно загрузить остатки товаров на Ozon.
    
    Args:
        watch_remnants (list): Список словарей с данными об остатках товаров.
        client_id (str): Идентификатор клиента Ozon
        seller_token (str): API-ключ Ozon
        
    Returns:
        tuple: Кортеж с двумя списками - товары с ненулевыми остатками
        и все товары с остатками
        
    Examples:
        >>> await upload_stocks(watch_remnants, "123", "456")
        (
            [
                {
                    "offer_id": "123", 
                    "stock": 10
                }
            ], 
            [
                {
                    "offer_id": "123",
                    "stock": 10
                }, 
                {
                    "offer_id": "456", 
                    "stock": 0
                }
            ]
        )
        
        >>> await upload_stocks([], "invalid", "token")
        requests.exceptions.HTTPError: 401 Client Error: Unauthorized
    """
    
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try: 
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()

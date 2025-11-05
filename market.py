import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Получить список товаров с Яндекс Маркета.
    
    Args:
        page (str): Токен страницы для пагинации.
        campaign_id (str): Идентификатор компании в Яндекс Маркете.
        access_token (str): Токен для API Яндекс Маркета.
        
    Returns:
        dict: Словарь с данными о товарах
        
    Examples:
        >>> get_product_list("", "campaign123", "token456")
        {
            "offerMappingEntries": [
                {"offer": {"shopSku": "123"}}
            ],
            "paging": {
                "nextPageToken": "abc123", 
                prevPageToken: "abc456"
            }
        }
        
        >>> get_product_list("", "invalid_campaign", "invalid_token")
        requests.exceptions.HTTPError: 401 Client Error: Unauthorized
    """
    
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """Обновить остатки товаров на Яндекс Маркете.
    
    Args:
        stocks (list): Список словарей с данными об остатках товаров.
        campaign_id (str): Идентификатор компании в Яндекс Маркете.
        access_token (str): Токен для API Яндекс Маркета.
        
    Returns:
        dict: Ответ от API Яндекс Маркета с результатом обновления остатков.
        
    Examples:
        >>> stocks = [
            {
                "sku": "123", 
                "warehouseId": "warehouse123",
                "items": [
                    {
                        "count": 10, 
                        "type": "FIT", 
                        "updatedAt": "2025-01-01T00:00:00Z"
                    }
                ]
            }
        ]
        >>> update_stocks(stocks, "campaign123", "token456")
        {"status": "OK"}
        
        >>> update_stocks([], "campaign123", "token456")
        requests.exceptions.HTTPError: 400 Client Error: Bad Request
    """
    
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """Обновить цены товаров на Яндекс Маркете.
    
    Args:
        prices (list): Список словарей с данными о ценах товаров.
        campaign_id (str): Идентификатор компании в Яндекс Маркете.
        access_token (str): Токен для API Яндекс Маркета.
        
    Returns:
        dict: Ответ от API Яндекс Маркета с результатом обновления цен.
        
    Examples:
        >>> prices =  [
            {
                "id": "123",
                "price": {
                    "value": 16590, 
                    "currencyId": "RUR"
                }
            }
        ]
        >>> update_price(prices, "campaign123", "token456")
        {"status": "OK"}
        
        >>> update_price([{"id": "invalid"}], "campaign123", "token456")
        requests.exceptions.HTTPError: 400 Client Error: Bad Request
    """
    
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Получить артикулы товаров Яндекс Маркета.
    
    Args:
        campaign_id (str): Идентификатор компании в Яндекс Маркете.
        market_token (str): Токен для API Яндекс Маркета.
        
    Returns:
        list: Список артикулов всех товаров компании.
        
    Examples:
        >>> get_offer_ids("campaign123", "token456")
        ["123", "456"]
        
        >>> get_offer_ids("invalid_campaign", "invalid_token")
        requests.exceptions.HTTPError: 401 Client Error: Unauthorized
    """
    
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """Создать список остатков товаров для обновления на Яндекс Маркете.
    
    Args:
        watch_remnants (list): Список словарей с данными об остатках товаров.
        offer_ids (list): Список артикулов товаров c Яндекс Маркета.
        warehouse_id (str): Идентификатор склада в Яндекс Маркете.
        
    Returns:
        list: Список словарей с данными об остатках для Яндекс Маркета.
        
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
        >>> create_stocks(watch_remnants, offer_ids, "warehouse123")
        [
            {
                "sku": "123",
                "warehouseId": "warehouse123",
                "items": [
                    {
                        "count": 100, 
                        "type": "FIT", 
                        "updatedAt": "2025-01-01T00:00:00Z"
                    }
                ]
            },
            {
                "sku": "456", 
                "warehouseId": "warehouse123",
                "items": [
                    {
                        "count": 0, 
                        "type": "FIT", 
                        "updatedAt": "2025-01-01T00:00:00Z"
                    }
                ]
            }
        ]
        
        >>> create_stocks([], ["123"], "warehouse123")
        [
            {
                "sku": "123",
                "warehouseId": "warehouse123", 
                "items": [
                    {
                        "count": 0, 
                        "type": "FIT", 
                        "updatedAt": "2025-01-01T00:00:00Z"
                    }
                ]
            }
        ]
    """
    
    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создать список цен товаров для обновления на Яндекс Маркете.
    
    Args:
        watch_remnants (list): Список словарей с данными о ценах товаров.
        offer_ids (list): Список артикулов товаров с Яндекс Маркета.
        
    Returns:
        list: Список словарей с данными о ценах для API Яндекс Маркета.
        
    Examples:
        >>> watch_remnants = [
            {
                "Код": "123",
                "Наименование товара": "BA-110-4A1"
                "Цена": "16'590.00 руб."
                "Количество": "5", 
            }
        ]
        >>> offer_ids = ["123"]
        >>> create_prices(watch_remnants, offer_ids)
        [
            {
                "id": "123",
                "price": {
                    "value": 16590, 
                    "currencyId": "RUR"
                }
            }
        ]
        
        >>> create_prices([], ["123"])
        []
    """
    
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """Асинхронно загрузить цены товаров на Яндекс Маркет.
    
    Args:
        watch_remnants (list): Список словарей с данными о ценах товаров.
        campaign_id (str): Идентификатор компании в Яндекс Маркете.
        market_token (str): Токен для API Яндекс Маркета.
        
    Returns:
        list: Список всех созданных цен для загрузки.
        
    Examples:
        >>> await upload_prices(watch_remnants, "campaign123", "token456")
        [
            {
                "id": "123",
                "price": {
                    "value": 16590, 
                    "currencyId": "RUR"
                }
            }
        ]
        
        >>> await upload_prices([], "invalid_campaign", "token456")
        requests.exceptions.HTTPError: 401 Client Error: Unauthorized
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """Асинхронно загрузить остатки товаров на Яндекс Маркет.
    
    Args:
        watch_remnants (list): Список словарей с данными об остатках товаров.
        campaign_id (str): Идентификатор компании в Яндекс Маркете.
        market_token (str): Токен для API Яндекс Маркета.
        warehouse_id (str): Идентификатор склада в Яндекс Маркете.
        
    Returns:
        tuple: Кортеж с двумя списками:
            - list: Товары с ненулевыми остатками
            - list: Все товары с остатками
            
    Examples:
        >>> await upload_stocks(watch_remnants, "campaign123", "token456", "warehouse123")
        (
            [
                {
                    "sku": "123",
                    "warehouseId": "warehouse123",
                    "items": [
                        {
                            "count": 100, 
                            "type": "FIT", 
                            "updatedAt": "2025-01-01T00:00:00Z"
                        }
                    ]
                }
            ],
            [
                {
                    "sku": "123",
                    "warehouseId": "warehouse123",
                    "items": [
                        {
                            "count": 100, 
                            "type": "FIT", 
                            "updatedAt": "2025-01-01T00:00:00Z"
                        }
                    ]
                },
                {
                    "sku": "456", 
                    "warehouseId": "warehouse123",
                    "items": [
                        {
                            "count": 0, 
                            "type": "FIT", 
                            "updatedAt": "2025-01-01T00:00:00Z"
                        }
                    ]
                }
            ]
        )
        
        >>> await upload_stocks([], "invalid", "token", "warehouse")
        requests.exceptions.HTTPError: 401 Client Error: Unauthorized
    """
    
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()

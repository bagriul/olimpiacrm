from pymongo import MongoClient
from datetime import datetime, timedelta
import requests
import xml.etree.ElementTree as ET
import config

client = MongoClient(config.MONGO_STRING)
db = client['olimpia_crm']
users_collection = db['users']
statuses_collection = db['statuses']
tasks_collection = db['tasks']
contracts_collection = db['contracts']
merchants_reports_collection = db['merchants_reports']
clients_collection = db['clients']
orders_collection = db['orders']
products_collection = db['products']
manufactured_products_collection = db['manufactured_products']
used_raw_collection = db['used_raw']
defective_products_collection = db['defective_products']
pallets_collection = db['pallets']


def total_sales(start_date, end_date, subwarehouse):
    pipeline = [
        {
            "$match": {
                "date": {"$gte": start_date, "$lte": end_date},
                "product.0.subwarehouse": subwarehouse  # Фільтруємо за першим продуктом
            }
        },
        {
            "$unwind": "$product"  # Розкладаємо масив product
        },
        {
            "$group": {
                "_id": None,
                "total_amount": {"$sum": "$product.amount"}  # Підсумовуємо кількість товарів
            }
        }
    ]
    result = list(orders_collection.aggregate(pipeline))
    total_amount = result[0]['total_amount'] if result else 0
    return total_amount


def average_order_amount(start_date, end_date, subwarehouse):
    pipeline = [
        {
            "$match": {
                "date": {"$gte": start_date, "$lte": end_date},
                "product.0.subwarehouse": subwarehouse  # Фільтруємо за першим продуктом
            }
        },
        {
            "$unwind": "$product"  # Розкладаємо масив product
        },
        {
            "$group": {
                "_id": "$order_number",  # Групуємо за номером замовлення
                "total_order_amount": {"$sum": "$product.amount"}  # Обчислюємо загальну кількість товарів у замовленні
            }
        },
        {
            "$group": {
                "_id": None,
                "total_order_count": {"$sum": 1},  # Підраховуємо загальну кількість замовлень
                "total_amount": {"$sum": "$total_order_amount"}  # Підраховуємо загальну кількість товарів для всіх замовлень
            }
        }
    ]

    result = list(orders_collection.aggregate(pipeline))

    total_order_count = result[0]['total_order_count'] if result else 0
    total_amount = result[0]['total_amount'] if result else 0

    average_order_amount = total_amount / total_order_count if total_order_count > 0 else 0
    return average_order_amount


def order_volume_dynamic(start_date, end_date, subwarehouse):
    # Конвертація start_date і end_date в об'єкти datetime
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')

    # Ініціалізуємо словник для зберігання кількості товарів по днях
    daily_product_amount = {}

    # Ітеруємо по кожному дню в заданому періоді
    current_date = start_date
    while current_date <= end_date:
        # Запит для пошуку замовлень на поточний день
        query = {
            'date': {
                '$gte': current_date.strftime('%Y-%m-%d 00:00:00'),
                '$lt': (current_date + timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')
            },
            'product.0.subwarehouse': subwarehouse  # Фільтруємо за першим продуктом у замовленні
        }

        # Агрегуємо загальну кількість продуктів на поточний день
        total_amount = 0
        for order in orders_collection.find(query):
            for product in order['product']:
                total_amount += product['amount']

        # Зберігаємо загальну кількість для поточного дня в словник
        daily_product_amount[current_date.strftime('%Y-%m-%d')] = total_amount

        # Переходимо до наступного дня
        current_date += timedelta(days=1)

    return daily_product_amount


def paid_orders_percentage(start_date, end_date, subwarehouse):
    # Конвертація start_date і end_date в об'єкти datetime
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')

    # Ініціалізуємо змінні
    total_orders = 0
    paid_orders = 0
    order_number_list = []  # Збираємо всі номери замовлень

    # Знайти всі замовлення в межах діапазону дат та фільтрувати за першим продуктом складу
    query = {
        'date': {
            '$gte': start_date,
            '$lte': end_date
        },
        'product.0.subwarehouse': subwarehouse  # Фільтруємо за першим продуктом у замовленні
    }

    # Проходимо по замовленнях, щоб підрахувати їх загальну кількість і номери
    for order in orders_collection.find(query):
        total_orders += 1
        order_number_list.append(order['order_number_1c'])

    # Відправляємо запит для отримання статусу оплати всіх замовлень
    request_payment = requests.post(
        'https://olimpia.comp.lviv.ua:8189/BaseWeb/hs/base?action=getpaymentstatus',
        data={"order": order_number_list}, auth=('CRM', 'CegJr6YcK1sTnljgTIly')
    )

    # Розбираємо XML-відповідь
    root = ET.fromstring(request_payment.text)

    # Перебираємо статуси оплати і підраховуємо кількість оплачених замовлень
    for payment_status in root.iter('status'):
        if payment_status.text == 'Оплачено':
            paid_orders += 1

    # Обчислюємо відсоток оплачених замовлень
    paid_percentage = (paid_orders / total_orders) * 100 if total_orders > 0 else 0

    return paid_percentage


def analyze_repeat_orders(start_date, end_date, subwarehouse):
    # Convert start_date and end_date to datetime objects
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')

    # Initialize dictionary to store counterparties and their repeat order information
    counterparties = {}

    # Iterate through each day in the given time period
    current_date = start_date
    while current_date <= end_date:
        # Construct query to find orders on the current day
        query = {
            'date': {
                '$gte': current_date.strftime('%Y-%m-%d 00:00:00'),
                '$lt': (current_date + timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')
            },
            'product.0.subwarehouse': subwarehouse
        }

        # Analyze orders for repeat counterparties
        for order in orders_collection.find(query):
            counterpartie_code = order['counterpartie_code']
            if counterpartie_code in counterparties:
                # Update existing counterparty entry
                counterparties[counterpartie_code]['order_count'] += 1
                counterparties[counterpartie_code]['total_amount'] += sum(
                    product['amount'] for product in order['product'])
            else:
                # Create new counterparty entry
                counterparties[counterpartie_code] = {
                    'order_count': 1,
                    'total_amount': sum(product['amount'] for product in order['product'])
                }

        # Move to the next day
        current_date += timedelta(days=1)

    return counterparties


def calculate_sales_agent_rating(start_date, end_date, subwarehouse):
    end_date = datetime.strptime(end_date, '%Y-%m-%d')

    # Construct query to find reports within the given time period
    query = {
        'date': {
            '$gte': start_date,
            '$lt': (end_date + timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')
        },
        'counterpartie_warehouse': subwarehouse
    }

    # Initialize dictionary to store counterpartie names and their total sale amount
    counterpartie_sales = {}

    # Aggregate total sale amount for each counterpartie
    for report in merchants_reports_collection.find(query):
        counterpartie_name = report['counterpartie_name']
        sale_amount = float(report['total_price_sum'])
        if counterpartie_name in counterpartie_sales:
            counterpartie_sales[counterpartie_name] += sale_amount
        else:
            counterpartie_sales[counterpartie_name] = sale_amount

    return counterpartie_sales


def calculate_product_rating(start_date, end_date, subwarehouse):
    # Convert end_date to a datetime object and add one day to include the end date
    end_date = datetime.strptime(end_date, '%Y-%m-%d')

    # Initialize dictionary to store sales amount for each product
    products = {}

    # Construct query to find reports within the given time period and subwarehouse
    query = {
        'date': {
            '$gte': start_date,
            '$lt': (end_date + timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')
        },
        'counterpartie_warehouse': subwarehouse
    }

    # Aggregate sales amount for each product from the reports
    for report in merchants_reports_collection.find(query):
        # Loop through each product in the 'products' array within the report
        for product in report['products']:
            product_name = product['product_name']
            sale_amount = float(product['product_price']) + float(
                product['sale_price'])  # Total price sum for the product

            if product_name in products:
                products[product_name] += sale_amount
            else:
                products[product_name] = sale_amount

    # Sort products by sales amount in descending order
    sorted_products = sorted(products.items(), key=lambda x: x[1], reverse=True)

    return sorted_products


def get_total_rest_by_warehouse(subwarehouse):
    pipeline = [
        {"$match": {"rest": {"$exists": True}}},
        {"$addFields": {
            "rest_corrected": {
                "$convert": {
                    "input": {"$replaceOne": {"input": "$rest", "find": ",", "replacement": "."}},
                    "to": "double",
                    "onError": 0  # Handle invalid values by setting them to 0
                }
            }
        }},
        {"$group": {
            "_id": {"warehouse": "$warehouse", "subwarehouse": "$subwarehouse"},
            "total_rest": {"$sum": "$rest_corrected"}
        }}
    ]

    results = products_collection.aggregate(pipeline)

    total_rests = {}
    for result in results:
        warehouse = result["_id"]["warehouse"]
        subwarehouse = result["_id"]["subwarehouse"]
        total_rest = result["total_rest"]

        if warehouse not in total_rests:
            total_rests[warehouse] = {}
        total_rests[warehouse][subwarehouse] = total_rest

    return total_rests


def get_total_price_for_workwear(start_date_str, end_date_str, subwarehouse):
    try:
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    except ValueError:
        return 'Invalid date format, expected YYYY-MM-DD'

    # Convert dates to match MongoDB's format
    start_date_formatted = start_date.strftime('%a %d %B %Y')
    end_date_formatted = end_date.strftime('%a %d %B %Y')

    query = {
        'warehouse': 'Склад Спецодягу',
        'subwarehouse': subwarehouse
    }

    total_price = 0
    documents = products_collection.find(query)
    for document in documents:
        sum = float(document['price']) * float(document['rest'])
        total_price += sum

    return total_price


def get_low_stock_products(subwarehouse):
    pipeline = [
        {
            "$match": {
                "recommended_rest": {"$exists": True, "$ne": ""},
                "rest": {"$exists": True, "$ne": ""},
                "subwarehouse": subwarehouse  # Filter by subwarehouse field
            }
        },
        {
            "$project": {
                "_id": 1,
                "code": 1,
                "good": 1,
                "rest": {
                    "$convert": {
                        "input": {
                            "$replaceOne": {
                                "input": "$rest", "find": ",", "replacement": "."
                            }
                        },
                        "to": "double",
                        "onError": 0  # Handle errors during conversion
                    }
                },
                "recommended_rest": {
                    "$convert": {
                        "input": {
                            "$replaceOne": {
                                "input": "$recommended_rest",
                                "find": ",", "replacement": "."
                            }
                        },
                        "to": "double",
                        "onError": 0
                    }
                }
            }
        },
        {
            "$match": {
                "$expr": {"$lt": ["$rest", "$recommended_rest"]}
            }
        }
    ]

    low_stock_products_aggregation = list(products_collection.aggregate(pipeline))
    low_stock_products = []
    for product in low_stock_products_aggregation:
        try:
            low_stock_products.append(product['good'])
        except KeyError:
            pass
    return low_stock_products


def get_products_with_expired_series(subwarehouse_value):
    today = datetime.now().strftime("%d.%m.%Y")  # Get today's date in the format dd.mm.yyyy

    pipeline = [
        {
            "$match": {
                "series": {"$exists": True, "$ne": ""},
                "subwarehouse": subwarehouse_value  # Filter by subwarehouse field
            }
        },
        {
            "$addFields": {
                "series_date": {
                    "$dateFromString": {
                        "dateString": "$series",
                        "format": "%d.%m.%Y"
                    }
                }
            }
        },
        {
            "$match": {
                "$expr": {"$lte": ["$series_date", {"$dateFromString": {"dateString": today}}]}
            }
        },
        {
            "$project": {
                "_id": 0,
                "good": 1
            }
        }
    ]

    result = products_collection.aggregate(pipeline)
    return [doc["good"] for doc in result]  # Return a list of product names


def get_total_amount_for_distributor(subwarehouse, target_warehouse="Склад Дистриб'ютора"):
    pipeline = [
        {"$match": {"warehouse": target_warehouse, "subwarehouse": subwarehouse,  "amount": {"$exists": True}}},
        {"$group": {
            "_id": None,
            "total_amount": {
                "$sum": {
                    "$convert": {
                        "input": {
                            "$replaceOne": {
                                "input": "$amount",
                                "find": ",",
                                "replacement": "."
                            }
                        },
                        "to": "double",
                        "onError": 0  # Handle invalid values
                    }
                }
            }
        }}
    ]

    result = products_collection.aggregate(pipeline)
    result_doc = next(result, None)
    if result_doc:
        return result_doc["total_amount"]
    else:
        return None


def get_total_amount_manufactured_by_good(start_date, end_date, subwarehouse):
    # Convert date strings to datetime objects
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    pipeline = [
        {
            "$match": {
                "date": {
                    "$gte": start_date,
                    "$lte": end_date
                },
                "subwarehouse": subwarehouse  # Додаємо фільтр за складом
            }
        },
        {
            "$group": {
                "_id": "$good",  # Групуємо за товаром (good)
                "total_amount": {"$sum": "$amount"}  # Сума кількості для кожного товару
            }
        }
    ]

    results = manufactured_products_collection.aggregate(pipeline)

    total_amounts = {}
    for result in results:
        good = result["_id"]
        total_amount = result["total_amount"]
        total_amounts[good] = total_amount

    return total_amounts


def get_total_used_raw(start_date_str, end_date_str, subwarehouse):
    # Convert date strings to datetime objects
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    pipeline = [
        {
            "$match": {
                "date": {
                    "$gte": start_date,
                    "$lte": end_date
                },
                "subwarehouse": subwarehouse  # Додаємо фільтр за складом
            }
        },
        {
            "$group": {
                "_id": None,
                "total_used": {"$sum": "$used"}
            }
        }
    ]

    result = used_raw_collection.aggregate(pipeline)
    result_doc = next(result, None)  # Get the first (and only) document
    if result_doc:
        return float(result_doc["total_used"])
    else:
        return None


def get_defect_raw_percentage(start_date_str, end_date_str, subwarehouse):
    total_used = get_total_used_raw(start_date_str, end_date_str, subwarehouse)

    if total_used is None or total_used == 0:
        return None  # Avoid division by zero

    # Convert date strings to datetime objects
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    pipeline = [
        {
            "$match": {
                "date": {
                    "$gte": start_date,
                    "$lte": end_date
                },
                "subwarehouse": subwarehouse  # Додаємо фільтр за складом
            }
        },
        {
            "$group": {
                "_id": None,
                "total_defect": {"$sum": "$defect"}
            }
        }
    ]

    result = used_raw_collection.aggregate(pipeline)
    result_doc = next(result, None)
    if result_doc:
        total_defect = float(result_doc["total_defect"])
        return (total_defect / total_used) * 100
    else:
        return None


def get_contracts_stats(start_date_str, end_date_str, subwarehouse):
    # Конвертуємо рядки дат (YYYY-MM-DD) у об'єкти datetime
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    seven_days_later = end_date + timedelta(days=7)
    now = datetime.now()

    pipeline = [
        {  # Розділяємо дату і дедлайн на частини та конвертуємо у об'єкти дати
            "$addFields": {
                "date_parts": {"$split": ["$date", " "]},
                "deadline_parts": {"$split": ["$deadline", " "]}
            }
        },
        {  # Конвертуємо частини дати та дедлайну у об'єкти дати
            "$addFields": {
                "date_obj": {
                    "$dateFromParts": {
                        "year": {"$toInt": {"$arrayElemAt": ["$date_parts", 3]}},
                        "month": {
                            "$switch": {
                                "branches": [
                                    {"case": {"$eq": [{"$arrayElemAt": ["$date_parts", 1]}, "Jan"]}, "then": 1},
                                    {"case": {"$eq": [{"$arrayElemAt": ["$date_parts", 1]}, "Feb"]}, "then": 2},
                                    {"case": {"$eq": [{"$arrayElemAt": ["$date_parts", 1]}, "Mar"]}, "then": 3},
                                    {"case": {"$eq": [{"$arrayElemAt": ["$date_parts", 1]}, "Apr"]}, "then": 4},
                                    {"case": {"$eq": [{"$arrayElemAt": ["$date_parts", 1]}, "May"]}, "then": 5},
                                    {"case": {"$eq": [{"$arrayElemAt": ["$date_parts", 1]}, "Jun"]}, "then": 6},
                                    {"case": {"$eq": [{"$arrayElemAt": ["$date_parts", 1]}, "Jul"]}, "then": 7},
                                    {"case": {"$eq": [{"$arrayElemAt": ["$date_parts", 1]}, "Aug"]}, "then": 8},
                                    {"case": {"$eq": [{"$arrayElemAt": ["$date_parts", 1]}, "Sep"]}, "then": 9},
                                    {"case": {"$eq": [{"$arrayElemAt": ["$date_parts", 1]}, "Oct"]}, "then": 10},
                                    {"case": {"$eq": [{"$arrayElemAt": ["$date_parts", 1]}, "Nov"]}, "then": 11},
                                    {"case": {"$eq": [{"$arrayElemAt": ["$date_parts", 1]}, "Dec"]}, "then": 12}
                                ],
                                "default": 1
                            }
                        },
                        "day": {"$toInt": {"$arrayElemAt": ["$date_parts", 2]}}
                    }
                },
                "deadline_date": {
                    "$dateFromParts": {
                        "year": {"$toInt": {"$arrayElemAt": ["$deadline_parts", 3]}},
                        "month": {
                            "$switch": {
                                "branches": [
                                    {"case": {"$eq": [{"$arrayElemAt": ["$deadline_parts", 1]}, "Jan"]}, "then": 1},
                                    {"case": {"$eq": [{"$arrayElemAt": ["$deadline_parts", 1]}, "Feb"]}, "then": 2},
                                    {"case": {"$eq": [{"$arrayElemAt": ["$deadline_parts", 1]}, "Mar"]}, "then": 3},
                                    {"case": {"$eq": [{"$arrayElemAt": ["$deadline_parts", 1]}, "Apr"]}, "then": 4},
                                    {"case": {"$eq": [{"$arrayElemAt": ["$deadline_parts", 1]}, "May"]}, "then": 5},
                                    {"case": {"$eq": [{"$arrayElemAt": ["$deadline_parts", 1]}, "Jun"]}, "then": 6},
                                    {"case": {"$eq": [{"$arrayElemAt": ["$deadline_parts", 1]}, "Jul"]}, "then": 7},
                                    {"case": {"$eq": [{"$arrayElemAt": ["$deadline_parts", 1]}, "Aug"]}, "then": 8},
                                    {"case": {"$eq": [{"$arrayElemAt": ["$deadline_parts", 1]}, "Sep"]}, "then": 9},
                                    {"case": {"$eq": [{"$arrayElemAt": ["$deadline_parts", 1]}, "Oct"]}, "then": 10},
                                    {"case": {"$eq": [{"$arrayElemAt": ["$deadline_parts", 1]}, "Nov"]}, "then": 11},
                                    {"case": {"$eq": [{"$arrayElemAt": ["$deadline_parts", 1]}, "Dec"]}, "then": 12}
                                ],
                                "default": 1
                            }
                        },
                        "day": {"$toInt": {"$arrayElemAt": ["$deadline_parts", 2]}}
                    }
                }
            }
        },
        {
            "$match": {
                "date_obj": {"$gte": start_date, "$lte": end_date},
                "subwarehouse": subwarehouse  # Додаємо фільтр за складом
            }
        },
        {
            "$facet": {
                "total_contracts": [{"$count": "count"}],
                "expiring_contracts": [
                    {
                        "$match": {
                            "$expr": {
                                "$or": [
                                    {"$and": [
                                        {"$gte": ["$deadline_date", end_date]},
                                        {"$lt": ["$deadline_date", seven_days_later]}
                                    ]},
                                    {"$lt": ["$deadline_date", now]}
                                ]
                            }
                        }
                    },
                    {"$project": {"number": 1, "_id": 0}}
                ]
            }
        }
    ]

    result = contracts_collection.aggregate(pipeline)
    result_doc = next(result, None)
    if result_doc:
        total_count = result_doc["total_contracts"][0]["count"] if result_doc["total_contracts"] else 0
        expiring_numbers = [doc["number"] for doc in result_doc["expiring_contracts"]]
        return total_count, expiring_numbers
    else:
        return 0, []


def sale_products_report(subwarehouse):
    sale_data = {}
    query = {}  # Initialize an empty query

    if subwarehouse:
        query["subwarehouse"] = subwarehouse  # Add subwarehouse filter if provided

    for report in merchants_reports_collection.find(query):
        try:
            # Iterate through the products in the report
            for product in report.get("products", []):
                if report.get("total_price_sum", 0) > 0:  # Check sale_amount
                    product_name = product.get("product_name")  # Get product name
                    sale_data[product_name] = {
                        "amount": float(product.get("product_amount", 0)) + float(product.get("sale_amount", 0)),  # Get sale amount
                        "price": report.get("total_price_sum", 0)     # Get sale price
                    }
        except (KeyError, ValueError):
            pass  # Handle any errors gracefully

    return sale_data


def defective_products_report(subwarehouse):
    defective_products_data = {}
    query = {}  # Initialize an empty query

    if subwarehouse:
        query["subwarehouse"] = subwarehouse  # Add subwarehouse filter if provided

    # Iterate over each document in the collection
    for document in defective_products_collection.find(query):
        # Iterate over each defective product in the document
        for defective_product in document["defective_products"]:
            product_name = defective_product["product_name"]
            amount = int(defective_product["amount"])
            total_price = float(defective_product["total_price"])

            # If the product is already in the dictionary, update its values
            if product_name in defective_products_data:
                defective_products_data[product_name]["amount"] += amount
                defective_products_data[product_name]["total_price"] += total_price
            else:
                # Otherwise, add the product to the dictionary
                defective_products_data[product_name] = {
                    "amount": amount,
                    "total_price": total_price
                }

    return defective_products_data


def pallets_report(subwarehouse):
    pallets_data = {}
    query = {}  # Initialize an empty query

    if subwarehouse:
        query["subwarehouse"] = subwarehouse  # Add subwarehouse filter if provided

    for document in pallets_collection.find(query):
        counterpartie = document["counterpartie_name"]
        if counterpartie not in pallets_data:
            pallets_data[counterpartie] = {
                "amount": 0,
                "price": 0
            }

        # Iterate over each pallet in the document
        for pallet in document["pallets"]:
            pallets_data[counterpartie]["amount"] += float(pallet["pallet_amount"])
            pallets_data[counterpartie]["price"] += float(pallet["pallet_total_price"])

    return pallets_data


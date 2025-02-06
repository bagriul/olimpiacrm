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
    """
    Calculate total sales between start_date and end_date (inclusive) for a given subwarehouse.

    Parameters:
        start_date (str): Start date in 'yyyy-mm-dd' format.
        end_date (str): End date in 'yyyy-mm-dd' format.
        subwarehouse (str): Subwarehouse name.

    Returns:
        total_amount (float): Sum of the 'total' field for matching orders.
    """
    # Convert the input date strings to datetime objects
    start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")

    pipeline = [
        # Convert the order's date string ("dd.mm.yyyy HH:MM:SS") into a date object
        {
            "$addFields": {
                "order_date": {
                    "$dateFromString": {
                        "dateString": "$date",
                        "format": "%d.%m.%Y %H:%M:%S"
                    }
                }
            }
        },
        # Filter documents by the converted order_date and subwarehouse.
        {
            "$match": {
                "order_date": {"$gte": start_date_obj, "$lte": end_date_obj},
                "subwarehouse": subwarehouse
            }
        },
        # Convert the total string (with comma as decimal separator) into a numeric value.
        {
            "$addFields": {
                "total_numeric": {
                    "$toDouble": {
                        "$replaceAll": {
                            "input": "$total",
                            "find": ",",
                            "replacement": "."
                        }
                    }
                }
            }
        },
        # Group all matching documents and sum up total_numeric.
        {
            "$group": {
                "_id": None,
                "total_amount": {"$sum": "$total_numeric"}
            }
        }
    ]

    result = list(orders_collection.aggregate(pipeline))
    total_amount = result[0]['total_amount'] if result else 0
    return total_amount


def average_order_amount(start_date, end_date, subwarehouse):
    """
    Calculate the average order amount (total goods amount per order) between
    start_date and end_date for a given subwarehouse.

    Parameters:
        start_date (str): Start date in 'yyyy-mm-dd' format.
        end_date (str): End date in 'yyyy-mm-dd' format.
        subwarehouse (str): Subwarehouse name.

    Returns:
        average_order_amount (float): The average total goods amount per order.
    """
    # Convert input date strings to datetime objects.
    start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")

    pipeline = [
        # Convert the document's "date" string to a date object ("order_date")
        {
            "$addFields": {
                "order_date": {
                    "$dateFromString": {
                        "dateString": "$date",
                        "format": "%d.%m.%Y %H:%M:%S"
                    }
                }
            }
        },
        # Filter documents by order_date and subwarehouse.
        {
            "$match": {
                "order_date": {"$gte": start_date_obj, "$lte": end_date_obj},
                "subwarehouse": subwarehouse
            }
        },
        # Unwind the goods array
        {
            "$unwind": "$goods"
        },
        # Group by order number to calculate the total goods amount per order.
        {
            "$group": {
                "_id": "$number",  # Group by order number.
                "total_order_amount": {
                    "$sum": { "$toDouble": "$goods.amount" }  # Convert amount to number.
                }
            }
        },
        # Group all orders to calculate overall count and sum of goods amounts.
        {
            "$group": {
                "_id": None,
                "total_order_count": {"$sum": 1},
                "total_amount": {"$sum": "$total_order_amount"}
            }
        }
    ]

    result = list(orders_collection.aggregate(pipeline))
    if result:
        total_order_count = result[0].get('total_order_count', 0)
        total_amount = result[0].get('total_amount', 0)
    else:
        total_order_count = 0
        total_amount = 0

    average_order_amt = total_amount / total_order_count if total_order_count > 0 else 0
    return average_order_amt


def order_volume_dynamic(start_date, end_date, subwarehouse):
    """
    Returns a dictionary mapping each day (as 'yyyy-mm-dd') to the total amount
    of goods in orders for that day, filtered by the provided subwarehouse.

    Parameters:
        start_date (str): Start date in 'yyyy-mm-dd' format.
        end_date (str): End date in 'yyyy-mm-dd' format.
        subwarehouse (str): The subwarehouse to filter by.

    Returns:
        dict: Keys are date strings (yyyy-mm-dd) and values are the summed goods amounts.
    """
    # Convert the input date strings to datetime objects.
    start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
    # For an inclusive end_date, use an exclusive upper bound by adding one day.
    end_date_obj = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)

    pipeline = [
        # Convert the stored "date" string into a date object.
        {
            "$addFields": {
                "order_date": {
                    "$dateFromString": {
                        "dateString": "$date",
                        "format": "%d.%m.%Y %H:%M:%S"
                    }
                }
            }
        },
        # Filter documents by the converted order_date and subwarehouse.
        {
            "$match": {
                "order_date": {"$gte": start_date_obj, "$lt": end_date_obj},
                "subwarehouse": subwarehouse
            }
        },
        # Unwind the goods array so each item is processed individually.
        {
            "$unwind": "$goods"
        },
        # Convert the "amount" field of the goods item (stored as a string with a comma)
        # to a numeric value. Since the field is nested in "goods", we can use dot-notation.
        {
            "$addFields": {
                "goods.amount_numeric": {
                    "$toDouble": {
                        "$replaceAll": {
                            "input": "$goods.amount",
                            "find": ",",
                            "replacement": "."
                        }
                    }
                }
            }
        },
        # Group by day (formatted as 'yyyy-mm-dd') and sum the numeric amounts.
        {
            "$group": {
                "_id": {
                    "day": {
                        "$dateToString": {"format": "%Y-%m-%d", "date": "$order_date"}
                    }
                },
                "daily_amount": {"$sum": "$goods.amount_numeric"}
            }
        },
        # Sort the results by day.
        {
            "$sort": {"_id.day": 1}
        }
    ]

    result = list(orders_collection.aggregate(pipeline))
    # Transform the aggregation result into a dictionary mapping day to the summed amount.
    daily_product_amount = {doc['_id']['day']: doc['daily_amount'] for doc in result}
    return daily_product_amount


def paid_orders_percentage(start_date, end_date, subwarehouse):
    """
    Calculate the percentage of paid orders between start_date and end_date
    for a given subwarehouse. The orders collection stores dates as strings
    in the "dd.mm.yyyy HH:MM:SS" format, and the subwarehouse is stored at the top level.

    Parameters:
        start_date (str): Start date in "yyyy-mm-dd" format.
        end_date (str): End date in "yyyy-mm-dd" format.
        subwarehouse (str): Subwarehouse name.

    Returns:
        float: The percentage of orders that have been paid.
    """
    # Convert input date strings to datetime objects.
    start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
    # Use an exclusive upper bound for end_date by adding one day.
    end_date_obj = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)

    # Use an aggregation pipeline to convert the "date" string to a date object and filter documents.
    pipeline = [
        # Convert the "date" field into a date object ("order_date")
        {
            "$addFields": {
                "order_date": {
                    "$dateFromString": {
                        "dateString": "$date",
                        "format": "%d.%m.%Y %H:%M:%S"
                    }
                }
            }
        },
        # Filter orders within the given date range and matching the subwarehouse.
        {
            "$match": {
                "order_date": {"$gte": start_date_obj, "$lt": end_date_obj},
                "subwarehouse": subwarehouse
            }
        }
    ]

    # Get all matching orders
    orders = list(orders_collection.aggregate(pipeline))
    total_orders = len(orders)

    # If no orders found, return 0%
    if total_orders == 0:
        return 0

    # Collect order numbers (using the "number" field as identifier)
    order_number_list = [order["number"] for order in orders]

    # Request payment statuses from the external service.
    request_payment = requests.post(
        'https://olimpia.comp.lviv.ua:8189/BaseWeb/hs/base?action=getpaymentstatus',
        data={"order": order_number_list},
        auth=('CRM', 'CegJr6YcK1sTnljgTIly')
    )

    # Parse the XML response.
    root = ET.fromstring(request_payment.text)

    # Count the number of orders with a payment status of "Оплачено".
    paid_orders = 0
    for payment_status in root.iter('status'):
        if payment_status.text == 'Оплачено':
            paid_orders += 1

    # Calculate the percentage of paid orders.
    paid_percentage = (paid_orders / total_orders) * 100
    return paid_percentage


def analyze_repeat_orders(start_date, end_date, subwarehouse):
    """
    Analyze repeat orders by counterparty in the given date range and subwarehouse.

    The function returns a dictionary mapping each counterparty code to an object
    containing:
      - order_count: the number of orders
      - total_amount: the sum of goods amounts (converted from string to number)

    The orders are filtered by converting the stored "date" field (format "dd.mm.yyyy HH:MM:SS")
    to a date object and comparing it to the provided start_date and end_date (format "yyyy-mm-dd").

    Parameters:
      start_date (str): Start date in "yyyy-mm-dd" format.
      end_date   (str): End date in "yyyy-mm-dd" format.
      subwarehouse (str): Subwarehouse name.

    Returns:
      dict: A dictionary where keys are counterparty codes and values are dicts with
            "order_count" and "total_amount".
    """
    # Convert input date strings to datetime objects.
    start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
    # Use an exclusive upper bound for the end_date by adding one day.
    end_date_obj = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)

    pipeline = [
        # 1. Convert the stored "date" field to a proper date object ("order_date").
        {
            "$addFields": {
                "order_date": {
                    "$dateFromString": {
                        "dateString": "$date",
                        "format": "%d.%m.%Y %H:%M:%S"
                    }
                }
            }
        },
        # 2. Filter orders by the converted date and by the top-level "subwarehouse" field.
        {
            "$match": {
                "order_date": {"$gte": start_date_obj, "$lt": end_date_obj},
                "subwarehouse": subwarehouse
            }
        },
        # 3. For each order, compute the total amount by summing the amounts from the "goods" array.
        #    Each good's "amount" is stored as a string with a comma as the decimal separator.
        {
            "$addFields": {
                "order_total_amount": {
                    "$sum": {
                        "$map": {
                            "input": "$goods",
                            "as": "g",
                            "in": {
                                "$toDouble": {
                                    "$replaceAll": {
                                        "input": "$$g.amount",
                                        "find": ",",
                                        "replacement": "."
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
        # 4. Group orders by counterparty code and accumulate order count and total amount.
        {
            "$group": {
                "_id": "$counterpartie_code",
                "order_count": {"$sum": 1},
                "total_amount": {"$sum": "$order_total_amount"}
            }
        }
    ]

    results = orders_collection.aggregate(pipeline)

    # Convert the aggregation result into a dictionary.
    counterparties = {}
    for doc in results:
        counterparties[doc["_id"]] = {
            "order_count": doc["order_count"],
            "total_amount": doc["total_amount"]
        }

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


def get_total_rest_by_warehouse():
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
                "subwarehouse": subwarehouse  # Filter by subwarehouse
            }
        },
        {
            "$group": {
                "_id": None,
                "total_used": {"$sum": "$amount"}  # Sum the 'amount' field
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

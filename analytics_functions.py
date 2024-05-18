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
defective_pallets_collection = db['defective_pallets']


def total_sales(start_date, end_date):
    pipeline = [
        {
            "$match": {
                "date": {"$gte": start_date, "$lte": end_date},
            }
        },
        {
            "$unwind": "$product"  # Unwind the product array
        },
        {
            "$group": {
                "_id": None,
                "total_amount": {"$sum": "$product.amount"}  # Sum up the amounts
            }
        }
    ]
    result = list(orders_collection.aggregate(pipeline))
    total_amount = result[0]['total_amount'] if result else 0
    return total_amount


def average_order_amount(start_date, end_date):
    pipeline = [
        {
            "$match": {
                "date": {"$gte": start_date, "$lte": end_date}
            }
        },
        {
            "$unwind": "$product"  # Unwind the product array
        },
        {
            "$group": {
                "_id": "$order_number",  # Group by order number
                "total_order_amount": {"$sum": "$product.amount"}  # Calculate total order amount
            }
        },
        {
            "$group": {
                "_id": None,
                "total_order_count": {"$sum": 1},  # Count total number of orders
                "total_amount": {"$sum": "$total_order_amount"}  # Calculate total amount of all orders
            }
        }
    ]

    result = list(orders_collection.aggregate(pipeline))

    total_order_count = result[0]['total_order_count'] if result else 0
    total_amount = result[0]['total_amount'] if result else 0

    average_order_amount = total_amount / total_order_count if total_order_count > 0 else 0
    return average_order_amount


def order_volume_dynamic(start_date, end_date):
    # Convert start_date and end_date to datetime objects
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')

    # Initialize dictionary to store daily product amounts
    daily_product_amount = {}

    # Iterate through each day in the given time period
    current_date = start_date
    while current_date <= end_date:
        # Construct query to find orders on the current day
        query = {
            'date': {
                '$gte': current_date.strftime('%Y-%m-%d 00:00:00'),
                '$lt': (current_date + timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')
            }
        }

        # Aggregate total amount of products for the current day
        total_amount = 0
        for order in orders_collection.find(query):
            for product in order['product']:
                total_amount += product['amount']

        # Store total amount for the current day in the dictionary
        daily_product_amount[current_date.strftime('%Y-%m-%d')] = total_amount

        # Move to the next day
        current_date += timedelta(days=1)

    return daily_product_amount


def paid_orders_percentage(start_date, end_date):
    # Convert start_date and end_date to datetime objects
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')

    # Initialize variables
    total_orders = 0
    paid_orders = 0
    order_number_list = []  # Collect all order numbers

    # Find all orders within the date range and store their numbers
    query = {
        'date': {
            '$gte': start_date,
            '$lte': end_date
        }
    }
    for order in orders_collection.find(query):
        total_orders += 1
        order_number_list.append(order['order_number_1c'])

        # Make a single request with all order numbers
    request_payment = requests.post(
        'https://olimpia.comp.lviv.ua:8189/BaseWeb/hs/base?action=getpaymentstatus',
        data={"order": order_number_list}, auth=('CRM', 'CegJr6YcK1sTnljgTIly'))

    root = ET.fromstring(request_payment.text)

    # Iterate through the response to count paid orders
    for payment_status in root.iter('status'):
        if payment_status.text == 'Оплачено':
            paid_orders += 1

    # Calculate percentage of paid orders
    paid_percentage = (paid_orders / total_orders) * 100 if total_orders > 0 else 0

    return paid_percentage


def analyze_repeat_orders(start_date, end_date):
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
            }
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


def calculate_sales_agent_rating(start_date, end_date):
    end_date = datetime.strptime(end_date, '%Y-%m-%d')

    # Construct query to find reports within the given time period
    query = {
        'date': {
            '$gte': start_date,
            '$lt': (end_date + timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')
        }
    }

    # Initialize dictionary to store counterpartie names and their total sale amount
    counterpartie_sales = {}

    # Aggregate total sale amount for each counterpartie
    for report in merchants_reports_collection.find(query):
        counterpartie_name = report['counterpartie_name']
        sale_amount = int(report['sale_amount'])
        if counterpartie_name in counterpartie_sales:
            counterpartie_sales[counterpartie_name] += sale_amount
        else:
            counterpartie_sales[counterpartie_name] = sale_amount

    return counterpartie_sales


def calculate_product_rating(start_date, end_date):
    end_date = datetime.strptime(end_date, '%Y-%m-%d')

    # Initialize dictionary to store sales amount for each product
    products = {}

    # Construct query to find reports within the given time period
    query = {
        'date': {
            '$gte': start_date,
            '$lt': (end_date + timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')
        }
    }

    # Aggregate sales amount for each product
    for report in merchants_reports_collection.find(query):
        product_name = report['product_name']
        sale_amount = int(report['sale_amount'])
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


def get_total_price_for_workwear(target_warehouse="Склад Спецодягу"):
    pipeline = [
        {"$match": {"warehouse": target_warehouse, "price": {"$exists": True}}},
        {"$group": {
            "_id": None,
            "total_price": {
                "$sum": {
                    "$convert": {
                        "input": {
                            "$replaceOne": {
                                "input": "$price",
                                "find": ",",
                                "replacement": "."
                            }
                        },
                        "to": "double",
                        "onError": 0
                    }
                }
            }
        }}
    ]

    result = products_collection.aggregate(pipeline)
    result_doc = next(result, None)  # Get the first (and only) document
    if result_doc:
        return result_doc["total_price"]
    else:
        return None


def get_low_stock_products():
    pipeline = [
        {
            "$match": {
                "recommended_rest": {"$exists": True, "$ne": ""},
                "rest": {"$exists": True, "$ne": ""}
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
                                "find": ",",
                                "replacement": "."
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
        low_stock_products.append(product['good'])
    return low_stock_products


def get_products_with_expired_series():
    today = datetime.now().strftime("%d.%m.%Y")  # Get today's date in the format dd.mm.yyyy

    pipeline = [
        {
            "$match": {
                "series": {"$exists": True, "$ne": ""}
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
                # Change $gte to $lte
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


def get_total_amount_for_distributor(target_warehouse="Склад Дистриб'ютора"):
    pipeline = [
        {"$match": {"warehouse": target_warehouse, "amount": {"$exists": True}}},
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


def get_total_amount_manufactured_by_good(start_date, end_date):
    # Convert date strings to datetime objects
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    pipeline = [
        {
            "$match": {
                "date": {
                    "$gte": start_date,
                    "$lte": end_date
                }
            }
        },
        {
            "$group": {
                "_id": "$good",
                "total_amount": {"$sum": "$amount"}
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


def get_total_used_raw(start_date_str, end_date_str):
    # Convert date strings to datetime objects
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    pipeline = [
        {
            "$match": {
                "date": {
                    "$gte": start_date,
                    "$lte": end_date
                }
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


def get_defect_raw_percentage(start_date_str, end_date_str):
    total_used = get_total_used_raw(start_date_str, end_date_str)

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
                }
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


def get_contracts_stats(start_date_str, end_date_str):
    # Convert input date strings (YYYY-MM-DD) to datetime objects
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    seven_days_later = end_date + timedelta(days=7)
    now = datetime.now()

    pipeline = [
        {  # Split the date and parse parts separately
            "$addFields": {
                "date_parts": {"$split": ["$date", " "]},
                "deadline_parts": {"$split": ["$deadline", " "]}
            }
        },
        {  # Convert the date parts into date objects
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
                "date_obj": {"$gte": start_date, "$lte": end_date}
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


def sale_products_report():
    sale_data = {}
    for report in merchants_reports_collection.find():
        if report["sale_amount"] > 0:
            product_name = report["product_name"]
            sale_data[product_name] = {
                "amount": report["sale_amount"],
                "price": report["sale_price"]
            }
    return sale_data


def defective_products_report():
    defective_products_data = {}
    for defective_product in defective_products_collection.find():
        product_name = defective_product["product_name"]
        defective_products_data[product_name] = {
            "amount": defective_product["amount"],
            "price": defective_product["price"]
        }
    return defective_products_data


def pallets_report():
    pallets_data = {}
    for pallet in defective_pallets_collection.find():
        counterpartie = pallet["counterpartie"]
        pallets_data[counterpartie] = {
            "amount": pallet["amount"],
            "price": pallet["price"]
        }
    return pallets_data

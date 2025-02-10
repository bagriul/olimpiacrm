#!/usr/bin/env python3
"""
Telegram Bot for Olimpia CRM Operations

This bot supports:
  - Merchandiser reporting
  - Order creation
  - Manufactured products reporting
  - Raw material usage reporting
  - Defective products reporting
  - Pallets reporting

It uses MongoDB for data persistence, requests and XML parsing for external warehouse queries.
"""

import os
import base64
import traceback
from io import BytesIO
from uuid import uuid4
from datetime import datetime
import xml.etree.ElementTree as ET
import random

import requests
import pymongo
import telebot
from telebot import types

import config

# ---------------------- MongoDB Setup ---------------------- #
mongo_client = pymongo.MongoClient(config.MONGO_STRING)
db = mongo_client['olimpia_crm']
merchants_reports_collection = db['merchants_reports']
orders_collection = db['orders']
counterparties_collection = db['counterparties']
manufactured_products_collection = db['manufactured_products']
used_raw_collection = db['used_raw']
defective_products_collection = db['defective_products']
pallets_collection = db['pallets']

# -------------------- Telegram Bot Setup ------------------- #
bot = telebot.TeleBot(config.bot_token)

# Main menu keyboard
main_menu_markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
main_menu_markup.row(
    types.KeyboardButton("Звіт мерчандайзера"),
    types.KeyboardButton("Створити замовлення")
)
main_menu_markup.row(
    types.KeyboardButton("Кількість виробленої продукції"),
    types.KeyboardButton("Кількість використаної сировини")
)
main_menu_markup.row(
    types.KeyboardButton("Бракована продукція"),
    types.KeyboardButton("Піддони")
)


# ------------------ Utility Functions ---------------------- #
def get_warehouse_data(warehouse_name):
    """
    Fetch and parse XML data from a warehouse.

    :param warehouse_name: 'Етрус' or 'Фастпол'
    :return: ElementTree root element of the XML response.
    :raises ValueError: for unknown warehouse name.
    :raises Exception: if HTTP request fails.
    """
    if warehouse_name == 'Етрус':
        url = 'https://olimpia.comp.lviv.ua:8189/BaseWeb/hs/base?action=getreportrest'
    elif warehouse_name == 'Фастпол':
        url = 'https://olimpia.comp.lviv.ua:8189/BaseWeb1/hs/base?action=getreportrest'
    else:
        raise ValueError(f"Невідомий склад: {warehouse_name}")

    response = requests.get(url, auth=('CRM', 'CegJr6YcK1sTnljgTIly'), verify=False)
    if response.status_code != 200:
        raise Exception(f"Помилка запиту: {response.status_code}")

    return ET.fromstring(response.text)


def cancel_handler(message):
    """
    Cancel current operation and return to main menu.
    """
    counterpartie = counterparties_collection.find_one({"telegramID": message.from_user.id})
    if counterpartie:
        bot.send_message(message.chat.id, "Головне меню", reply_markup=main_menu_markup)
    else:
        bot.send_message(message.chat.id, 'Контрагента не знайдено')


# ------------------- Start and Contact Handlers ------------------- #
@bot.message_handler(commands=['start'])
def start(message):
    markup = types.ReplyKeyboardMarkup(one_time_keyboard=True, resize_keyboard=True)
    markup.add(types.KeyboardButton('Надіслати номер телефону', request_contact=True))
    bot.send_message(
        message.chat.id,
        'Поділіться, будь ласка, вашим номером телефону натиснувши кнопку нижче',
        reply_markup=markup
    )


@bot.message_handler(content_types=['contact'])
def handle_contact(message):
    phone_number = message.contact.phone_number
    counterpartie = counterparties_collection.find_one({"phone_number": phone_number})
    if counterpartie:
        counterparties_collection.find_one_and_update(
            {"phone_number": phone_number},
            {'$set': {'telegramID': message.from_user.id}}
        )
        bot.send_message(message.chat.id, "Головне меню", reply_markup=main_menu_markup)
    else:
        bot.send_message(message.chat.id, 'Контрагента не знайдено')


@bot.message_handler(commands=['cancel'])
def cancel_command(message):
    cancel_handler(message)


# ----------------- Merchandiser Report Handlers ----------------- #
# Global variables for merchandiser report
products = []
name_shop = ""
selected_subwarehouse = ""
name_product = ""
amount_product = ""
price_product = ""
amount_sale = ""
price_sale = ""
photo_base64 = None


@bot.message_handler(func=lambda message: message.text == "Звіт мерчандайзера", content_types=['text'])
def handle_merch_report(message):
    global products
    products = []  # Clear products list for new report
    bot.send_message(message.chat.id, "Яка назва торгової точки?")
    bot.register_next_step_handler(message, collect_shop_name)


def collect_shop_name(message):
    if message.text == '/cancel':
        cancel_handler(message)
    else:
        global name_shop
        name_shop = message.text
        markup = types.ReplyKeyboardMarkup(one_time_keyboard=True)
        markup.add(types.KeyboardButton('Фастпол'), types.KeyboardButton('Етрус'))
        bot.reply_to(message, "Оберіть субсклад (Фастпол або Етрус):", reply_markup=markup)
        bot.register_next_step_handler(message, collect_subwarehouse)


def collect_subwarehouse(message):
    if message.text == '/cancel':
        cancel_handler(message)
    elif message.text in ['Фастпол', 'Етрус']:
        global selected_subwarehouse
        selected_subwarehouse = message.text
        bot.reply_to(message, "Яка назва товару?")
        bot.register_next_step_handler(message, collect_product_name)
    else:
        bot.reply_to(message, "Будь ласка, оберіть один із варіантів: Фастпол або Етрус")
        bot.register_next_step_handler(message, collect_subwarehouse)


def collect_product_name(message):
    if message.text == '/cancel':
        cancel_handler(message)
    else:
        global name_product
        name_product = message.text
        bot.reply_to(message, "Яка кількість товару?")
        bot.register_next_step_handler(message, collect_product_amount)


def collect_product_amount(message):
    if message.text == '/cancel':
        cancel_handler(message)
    else:
        global amount_product
        amount_product = message.text
        bot.reply_to(message, 'Яка вартість товару?')
        bot.register_next_step_handler(message, collect_product_price)


def collect_product_price(message):
    if message.text == '/cancel':
        cancel_handler(message)
    else:
        global price_product
        price_product = message.text
        bot.reply_to(message, 'Яка кількість акційного товару?')
        bot.register_next_step_handler(message, collect_sale_amount)


def collect_sale_amount(message):
    if message.text == '/cancel':
        cancel_handler(message)
    else:
        global amount_sale
        amount_sale = message.text
        bot.reply_to(message, 'Яка вартість акційного товару?')
        bot.register_next_step_handler(message, collect_sale_price)


def collect_sale_price(message):
    if message.text == '/cancel':
        cancel_handler(message)
    else:
        global price_sale
        price_sale = message.text
        bot.reply_to(message, "Надішліть фото, або повідомлення 'скасувати'")
        bot.register_next_step_handler(message, process_photo)


def process_photo(message):
    global photo_base64
    if message.text == '/cancel':
        cancel_handler(message)
        return
    elif message.photo:
        # Take the highest resolution photo
        photo_file_id = message.photo[-1].file_id
        photo_info = bot.get_file(photo_file_id)
        photo_file = bot.download_file(photo_info.file_path)
        photo_bytes = BytesIO(photo_file)
        photo_base64 = base64.b64encode(photo_bytes.read()).decode('utf-8')
        bot.reply_to(message, "Фото отримано та збережено!")
    else:
        photo_base64 = None
        bot.reply_to(message, "Фото не отримано")

    # Append product information
    products.append({
        'product_name': name_product,
        'product_amount': amount_product,
        'product_price': price_product,
        'sale_amount': amount_sale,
        'sale_price': price_sale,
        'photo': photo_base64
    })

    markup = types.ReplyKeyboardMarkup(one_time_keyboard=True)
    markup.add(types.KeyboardButton('Додати ще товар'), types.KeyboardButton('Завершити'))
    bot.reply_to(
        message,
        "Ви можете додати ще один товар або завершити введення.",
        reply_markup=markup
    )


@bot.message_handler(func=lambda message: message.text.lower() in ['додати ще товар', 'завершити'])
def add_or_finish_product(message):
    if message.text.lower() == 'додати ще товар':
        bot.reply_to(message, "Яка назва товару?")
        bot.register_next_step_handler(message, collect_product_name)
    else:
        # Confirm all products
        bot.reply_to(message, "Ось інформація про всі товари:")
        for i, product in enumerate(products, start=1):
            bot.send_message(
                message.chat.id,
                (f"Товар {i}:\n"
                 f"Назва: {product['product_name']}\n"
                 f"Кількість: {float(product['product_amount'])}\n"
                 f"Вартість: {float(product['product_price'])}\n"
                 f"Кількість акційного товару: {float(product['sale_amount'])}\n"
                 f"Вартість акційного товару: {float(product['sale_price'])}")
            )
        markup = types.ReplyKeyboardMarkup(one_time_keyboard=True)
        markup.add(types.KeyboardButton('Підтвердити'), types.KeyboardButton('Скасувати'))
        bot.reply_to(
            message,
            "Якщо все гаразд, натисніть 'Підтвердити', щоб надіслати інформацію",
            reply_markup=markup
        )


@bot.message_handler(func=lambda message: message.text.lower() in ['підтвердити', 'скасувати'])
def confirm_merch_report(message):
    if message.text.lower() == 'підтвердити':
        counterpartie = counterparties_collection.find_one({'telegramID': message.from_user.id})
        total_sum = 0
        for product in products:
            prod_price = float(product['product_price'] or 0)
            sale_price = float(product['sale_price'] or 0)
            total_sum += (prod_price * float(product['product_amount'])) + (sale_price * float(product['sale_amount']))

        data_to_save = {
            'shop_name': name_shop,
            'subwarehouse': selected_subwarehouse,
            'products': products,
            'date': datetime.now().strftime('%Y-%m-%d'),
            'counterpartie_name': counterpartie['name'],
            'counterpartie_code': counterpartie['code'],
            'counterpartie_warehouse': counterpartie['warehouse'],
            'total_price_sum': total_sum
        }
        merchants_reports_collection.insert_one(data_to_save)
        bot.reply_to(message, "Інформація успішно надіслана")
        bot.send_message(message.chat.id, "Головне меню", reply_markup=main_menu_markup)
    else:
        bot.reply_to(message, "Операція скасована")
        bot.send_message(message.chat.id, "Головне меню", reply_markup=main_menu_markup)


# -------------------------------------------------------------------
# ORDER CREATION HANDLERS (New Order Document Structure)
# -------------------------------------------------------------------
# Global dictionary to store order data
user_data = {}


@bot.message_handler(func=lambda message: message.text == "Створити замовлення", content_types=['text'])
def choose_warehouse(message):
    keyboard = types.InlineKeyboardMarkup()
    keyboard.row(
        types.InlineKeyboardButton(text="Етрус", callback_data="warehouse_etrus"),
        types.InlineKeyboardButton(text="Фастпол", callback_data="warehouse_fastpol")
    )
    bot.send_message(message.from_user.id, 'Оберіть склад:', reply_markup=keyboard)


@bot.callback_query_handler(func=lambda call: call.data.startswith('warehouse_'))
def handle_warehouse_selection(call):
    if call.data == "warehouse_etrus":
        warehouse_name = 'Етрус'
        warehouse_short = 'e'
    elif call.data == "warehouse_fastpol":
        warehouse_name = 'Фастпол'
        warehouse_short = 'f'
    else:
        warehouse_name = user_data.get('goods', [{}])[0].get('subwarehouse', '')
        warehouse_short = 'e' if warehouse_name == 'Етрус' else 'f'

    # Store warehouse details for later use
    user_data['warehouse_name'] = warehouse_name
    user_data['warehouse_short'] = warehouse_short

    try:
        root = get_warehouse_data(warehouse_name)
    except Exception as e:
        bot.send_message(call.message.chat.id, f"Виникла помилка: {e}")
        return

    # Create an inline keyboard for product selection (filtering by Type '2')
    keyboard = types.InlineKeyboardMarkup()
    for product in root.findall('Product'):
        if product.get('Type') == '2':
            code = product.get('Code')
            good = product.get('Good')
            button = types.InlineKeyboardButton(
                text=good,
                callback_data=f"orderproduct_{code}_{warehouse_short}"
            )
            keyboard.add(button)

    try:
        bot.send_message(call.message.chat.id, 'Виберіть необхідний продукт', reply_markup=keyboard)
    except AttributeError:
        bot.send_message(call.from_user.id, 'Виберіть необхідний продукт', reply_markup=keyboard)


@bot.callback_query_handler(func=lambda call: call.data.startswith('orderproduct'))
def order_product_callback(call):
    parts = call.data.split('_')
    product_code = parts[1]
    warehouse_short = parts[2]
    # Determine subwarehouse from the warehouse short code
    subwarehouse = 'Фастпол' if warehouse_short == 'f' else 'Етрус'
    # Prepare a product dictionary; note that price and summ will be added later
    current_product = {'code': product_code, 'subwarehouse': subwarehouse}
    msg = bot.send_message(call.from_user.id, "Введіть кількість товару:")
    bot.register_next_step_handler(msg, process_order_amount, current_product)


def process_order_amount(message, current_product):
    if message.text == '/cancel':
        cancel_handler(message)
    else:
        # Store the amount as entered (we will convert later)
        current_product['amount'] = message.text
        msg = bot.send_message(message.chat.id, "Введіть ціну товару:")
        bot.register_next_step_handler(msg, process_order_price, current_product)


def process_order_price(message, current_product):
    if message.text == '/cancel':
        cancel_handler(message)
    else:
        try:
            # Allow both dot and comma as decimal separator
            price = float(message.text.replace(',', '.'))
        except ValueError:
            bot.send_message(message.chat.id, "Неправильне значення ціни. Будь ласка, спробуйте ще раз.")
            return

        # Format price as string with a comma for the decimal separator
        current_product['price'] = format(price, '.2f').replace('.', ',')
        try:
            amount = float(current_product['amount'].replace(',', '.'))
        except ValueError:
            amount = 0
        # Save the amount as string (without extra decimals)
        current_product['amount'] = str(int(amount)) if amount.is_integer() else str(amount)
        # Calculate summ = price * amount and format it similarly
        summ = price * amount
        current_product['summ'] = format(summ, '.2f').replace('.', ',')

        # Initialize goods list in user_data if needed and add this product
        if 'goods' not in user_data:
            user_data['goods'] = []
        user_data['goods'].append(current_product)

        msg = bot.send_message(message.chat.id, "Додати ще один продукт? (так/ні)")
        bot.register_next_step_handler(msg, check_order_add_more)


def check_order_add_more(message):
    if message.text == '/cancel':
        cancel_handler(message)
    elif message.text.strip().lower() == 'так':
        ask_product(message)
    else:
        msg = bot.send_message(message.from_user.id, 'Введіть коментар до замовлення (або залиште порожнім)')
        bot.register_next_step_handler(msg, process_order_comment)


def ask_product(message):
    """
    Re-send the inline keyboard with available products based on the stored warehouse.
    """
    warehouse_name = user_data.get('warehouse_name')
    warehouse_short = user_data.get('warehouse_short')
    try:
        root = get_warehouse_data(warehouse_name)
    except Exception as e:
        bot.send_message(message.chat.id, f"Виникла помилка: {e}")
        return

    keyboard = types.InlineKeyboardMarkup()
    for product in root.findall('Product'):
        if product.get('Type') == '2':
            code = product.get('Code')
            good = product.get('Good')
            button = types.InlineKeyboardButton(
                text=good,
                callback_data=f"orderproduct_{code}_{warehouse_short}"
            )
            keyboard.add(button)
    bot.send_message(message.from_user.id, 'Виберіть необхідний продукт', reply_markup=keyboard)


def process_order_comment(message):
    if message.text == '/cancel':
        cancel_handler(message)
    else:
        # Save comment; if empty string then use None
        user_data['comment'] = message.text if message.text.strip() != "" else None
        finalize_order(message)


def finalize_order(message):
    # Generate a (for example) random order number (as a string)
    order_number = str(random.randint(1, 1000))
    now = datetime.now()
    order_date = now.strftime("%d.%m.%Y %H:%M:%S")

    # Get buyer from counterparties (using telegramID)
    counterpartie = counterparties_collection.find_one({'telegramID': message.from_user.id})
    buyer = counterpartie['code'] if counterpartie and 'code' in counterpartie else ""

    # Calculate total from the sum of each good's "summ" value (convert comma to dot)
    total = 0.0
    for good in user_data.get('goods', []):
        try:
            total += float(good['summ'].replace(',', '.'))
        except Exception:
            pass
    total_str = format(total, '.2f').replace('.', ',')

    # Determine subwarehouse from the first good in the list
    subwarehouse = user_data.get('goods', [{}])[0].get('subwarehouse', '')

    # Build the new order document according to the new structure
    new_order = {
        "number": order_number,
        "date": order_date,
        "buyer": buyer,
        "total": total_str,
        "comment": user_data.get('comment'),
        "goods": user_data.get('goods', []),
        "subwarehouse": subwarehouse
    }

    # Save the order document to MongoDB (orders_collection)
    orders_collection.insert_one(new_order)
    bot.send_message(message.chat.id, "Дані успішно надіслано!")


# ---------------- Manufactured Products Reporting ---------------- #
@bot.message_handler(
    func=lambda message: message.text == "Кількість виробленої продукції",
    content_types=['text']
)
def choose_warehouse_for_manufactured(message):
    keyboard = types.InlineKeyboardMarkup()
    keyboard.row(
        types.InlineKeyboardButton(text="Етрус", callback_data="manufactured_warehouse_etrus"),
        types.InlineKeyboardButton(text="Фастпол", callback_data="manufactured_warehouse_fastpol")
    )
    bot.send_message(message.from_user.id, 'Оберіть склад:', reply_markup=keyboard)


@bot.callback_query_handler(func=lambda call: call.data.startswith('manufactured_warehouse_'))
def ask_manufactured_product(call):
    global user_data
    user_data = {'product': []}

    if call.data == "manufactured_warehouse_etrus":
        warehouse_name = 'Етрус'
        warehouse_short = 'e'
    elif call.data == "manufactured_warehouse_fastpol":
        warehouse_name = 'Фастпол'
        warehouse_short = 'f'

    try:
        root = get_warehouse_data(warehouse_name)
    except Exception as e:
        bot.send_message(call.message.chat.id, f"Виникла помилка: {e}")
        return

    keyboard = types.InlineKeyboardMarkup()
    for product in root.findall('Product'):
        if product.get('Type') == '2':
            code = product.get('Code')
            good = product.get('Good')
            button = types.InlineKeyboardButton(
                text=good,
                callback_data=f"mp_{code}_{warehouse_short}"
            )
            keyboard.add(button)

    bot.send_message(
        call.message.chat.id,
        'Виберіть необхідний продукт для внесення інформації про вироблену кількість',
        reply_markup=keyboard
    )


@bot.callback_query_handler(func=lambda call: call.data.startswith('mp_'))
def ask_manufactured_amount(call):
    parts = call.data.split('_')
    code = parts[1]
    warehouse_short = parts[2]
    warehouse_name = 'Етрус' if warehouse_short == 'e' else 'Фастпол'

    response = requests.get(
        'https://olimpia.comp.lviv.ua:8189/BaseWeb/hs/base?action=getreportrest'
        if warehouse_name == 'Етрус'
        else 'https://olimpia.comp.lviv.ua:8189/BaseWeb1/hs/base?action=getreportrest',
        auth=('CRM', 'CegJr6YcK1sTnljgTIly'),
        verify=False
    )
    root = ET.fromstring(response.text)
    for product in root.findall('Product'):
        if product.get('Code') == code:
            good = product.get('Good')
            break

    msg = bot.send_message(call.from_user.id, 'Введіть кількість виготовленої продукції')
    bot.register_next_step_handler(msg, confirm_manufactured_product, code, good, warehouse_name)


def confirm_manufactured_product(message, code, good, warehouse_name):
    if message.text == '/cancel':
        cancel_handler(message)
        return
    amount = int(message.text)
    user_data['product'].append({'code': code, 'amount': amount})
    counterpartie = counterparties_collection.find_one({'telegramID': message.from_user.id})
    base_web = 'BaseWeb' if warehouse_name == 'Етрус' else 'BaseWeb1'

    response = requests.post(
        f'https://olimpia.comp.lviv.ua:8189/{base_web}/hs/base?action=CreateProduction',
        json=user_data,
        auth=('CRM', 'CegJr6YcK1sTnljgTIly'),
        verify=False
    )
    root = ET.fromstring(response.text)
    answer = root.find('Answer').text
    production = root.find('production').text

    if answer == 'ok':
        manufactured_products_collection.insert_one({
            'date': datetime.now(),
            'document': production,
            'subwarehouse': warehouse_name,
            'code': code,
            'good': good,
            'amount': amount
        })
        bot.send_message(message.chat.id, "Дані успішно надіслано!")
    else:
        bot.send_message(message.from_user.id, 'Помилка надсилання даних')


# ----------------- Raw Material Usage Reporting ----------------- #
@bot.message_handler(
    func=lambda message: message.text == "Кількість використаної сировини",
    content_types=['text']
)
def choose_warehouse_for_raw_materials(message):
    keyboard = types.InlineKeyboardMarkup()
    keyboard.row(
        types.InlineKeyboardButton(text="Етрус", callback_data="raw_warehouse_etrus"),
        types.InlineKeyboardButton(text="Фастпол", callback_data="raw_warehouse_fastpol")
    )
    bot.send_message(message.from_user.id, 'Оберіть склад:', reply_markup=keyboard)


@bot.callback_query_handler(func=lambda call: call.data.startswith('raw_warehouse_'))
def ask_raw_material(call):
    global user_data
    user_data = {'product': []}
    if call.data == "raw_warehouse_etrus":
        warehouse_name = 'Етрус'
        warehouse_short = 'e'
    elif call.data == "raw_warehouse_fastpol":
        warehouse_name = 'Фастпол'
        warehouse_short = 'f'

    try:
        root = get_warehouse_data(warehouse_name)
    except Exception as e:
        bot.send_message(call.message.chat.id, f"Виникла помилка: {e}")
        return

    keyboard = types.InlineKeyboardMarkup()
    for product in root.findall('Product'):
        if product.get('Type') == '1':  # Filtering raw materials (type '1')
            code = product.get('Code')
            good = product.get('Good')
            button = types.InlineKeyboardButton(
                text=good,
                callback_data=f"usedraw_{code}_{warehouse_short}"
            )
            keyboard.add(button)

    bot.send_message(
        call.message.chat.id,
        'Виберіть необхідну сировину для внесення інформації про використання',
        reply_markup=keyboard
    )


@bot.callback_query_handler(func=lambda call: call.data.startswith('usedraw'))
def ask_used_raw(call):
    parts = call.data.split('_')
    raw_code = parts[1]
    warehouse_short = parts[2]

    msg = bot.send_message(call.from_user.id, 'Введіть кількість сировини, яка пішла у виробництво')
    bot.register_next_step_handler(msg, confirm_used_raw, raw_code, warehouse_short)


def confirm_used_raw(message, raw_code, warehouse_short):
    if message.text == '/cancel':
        cancel_handler(message)
        return
    used_amount = int(message.text)
    msg = bot.send_message(message.from_user.id, 'Введіть кількість браку')
    bot.register_next_step_handler(msg, process_defect_amount, used_amount, raw_code, warehouse_short)


def process_defect_amount(message, used_amount, raw_code, warehouse_short):
    if message.text == '/cancel':
        cancel_handler(message)
        return
    defect_amount = int(message.text)
    warehouse_name = 'Етрус' if warehouse_short == 'e' else "Фастпол"
    url = ('https://olimpia.comp.lviv.ua:8189/BaseWeb/hs/base?action=getreportrest'
           if warehouse_name == 'Етрус'
           else 'https://olimpia.comp.lviv.ua:8189/BaseWeb1/hs/base?action=getreportrest')
    response = requests.get(url, auth=('CRM', 'CegJr6YcK1sTnljgTIly'), verify=False)
    root = ET.fromstring(response.text)
    raw_name = None
    for product in root.findall('Product'):
        if product.get('Type') == '1' and product.get('Code') == raw_code:
            raw_name = product.get('Good')
            break

    used_raw_collection.insert_one({
        'date': datetime.now(),
        'code': raw_code,
        'good': raw_name,
        'amount': used_amount,
        'defect': defect_amount,
        'subwarehouse': warehouse_name
    })
    bot.send_message(message.from_user.id, 'Дані успішно надіслано')


# ------------------ Defective Products Reporting ------------------ #
defective_products = []


@bot.message_handler(func=lambda message: message.text == "Бракована продукція", content_types=['text'])
def handle_defective_products(message):
    global defective_products
    defective_products = []  # Reset list for new entry
    bot.send_message(message.chat.id, "Яка назва продукту?")
    bot.register_next_step_handler(message, collect_defective_product_name, {})


def collect_defective_product_name(message, product_data):
    if message.text == '/cancel':
        cancel_handler(message)
    else:
        product_data['product_name'] = message.text
        bot.reply_to(message, "Яка дата повернення? (У форматі рік-місяць-день)")
        bot.register_next_step_handler(message, collect_return_date, product_data)


def collect_return_date(message, product_data):
    if message.text == '/cancel':
        cancel_handler(message)
    else:
        product_data['return_date'] = message.text
        bot.reply_to(message, "Яка кількість?")
        bot.register_next_step_handler(message, collect_defective_amount, product_data)


def collect_defective_amount(message, product_data):
    if message.text == '/cancel':
        cancel_handler(message)
    else:
        product_data['amount'] = message.text
        bot.reply_to(message, "Яка загальна вартість?")
        bot.register_next_step_handler(message, collect_defective_price, product_data)


def collect_defective_price(message, product_data):
    if message.text == '/cancel':
        cancel_handler(message)
    else:
        product_data['total_price'] = message.text
        defective_products.append(product_data)
        markup = types.ReplyKeyboardMarkup(one_time_keyboard=True)
        markup.add(types.KeyboardButton('Додати ще'), types.KeyboardButton('Закінчити'))
        bot.reply_to(
            message,
            "Ви можете додати ще одну браковану продукцію або завершити введення.",
            reply_markup=markup
        )


@bot.message_handler(func=lambda message: message.text.lower() in ['додати ще', 'закінчити'])
def add_or_finish_defective(message):
    if message.text.lower() == 'додати ще':
        new_product_data = {}
        bot.reply_to(message, "Яка назва продукту?")
        bot.register_next_step_handler(message, collect_defective_product_name, new_product_data)
    else:
        bot.reply_to(message, "Ось інформація про всі браковані продукти:")
        for i, defective in enumerate(defective_products, start=1):
            bot.send_message(
                message.chat.id,
                (f"Продукт {i}:\n"
                 f"Назва: {defective['product_name']}\n"
                 f"Дата повернення: {defective['return_date']}\n"
                 f"Кількість: {float(defective['amount'])}\n"
                 f"Загальна вартість: {float(defective['total_price'])}")
            )
        markup = types.ReplyKeyboardMarkup(one_time_keyboard=True)
        markup.add(types.KeyboardButton('Фастпол'), types.KeyboardButton('Етрус'))
        bot.reply_to(message, "Оберіть субсклад (Фастпол або Етрус):", reply_markup=markup)
        bot.register_next_step_handler(message, confirm_defective_products)


def confirm_defective_products(message):
    selected_subwarehouse = message.text  # 'Фастпол' or 'Етрус'
    counterpartie = counterparties_collection.find_one({'telegramID': message.from_user.id})
    data_to_save = {
        'defective_products': defective_products,
        'date': datetime.now().strftime('%Y-%m-%d'),
        'counterpartie_name': counterpartie['name'],
        'counterpartie_code': counterpartie['code'],
        'subwarehouse': selected_subwarehouse
    }
    defective_products_collection.insert_one(data_to_save)
    bot.reply_to(message, "Інформація про браковану продукцію успішно надіслана")
    bot.send_message(message.chat.id, "Головне меню", reply_markup=main_menu_markup)


# ---------------------- Pallets Reporting ---------------------- #
pallets = []
counterpartie_name = ""
pallet_amount_value = ""
pallet_total_price_value = ""
selected_subwarehouse_value = ""


@bot.message_handler(func=lambda message: message.text == "Піддони", content_types=['text'])
def handle_pallets(message):
    global pallets
    pallets = []  # Reset for new entry
    counterpartie = counterparties_collection.find_one({'telegramID': message.from_user.id})
    if not counterpartie:
        bot.reply_to(message, "Контрагент не знайдений. Спробуйте ще раз або зверніться до підтримки.")
        return
    global counterpartie_name
    counterpartie_name = counterpartie['name']
    bot.send_message(message.chat.id, "Яка кількість піддонів?")
    bot.register_next_step_handler(message, collect_pallet_amount)


def collect_pallet_amount(message):
    if message.text == '/cancel':
        cancel_handler(message)
    else:
        global pallet_amount_value
        pallet_amount_value = message.text
        bot.reply_to(message, "Яка загальна вартість піддонів?")
        bot.register_next_step_handler(message, collect_pallet_total_price)


def collect_pallet_total_price(message):
    if message.text == '/cancel':
        cancel_handler(message)
    else:
        global pallet_total_price_value
        pallet_total_price_value = message.text
        pallets.append({
            'counterpartie_name': counterpartie_name,
            'pallet_amount': pallet_amount_value,
            'pallet_total_price': pallet_total_price_value
        })
        markup = types.ReplyKeyboardMarkup(one_time_keyboard=True)
        markup.add(types.KeyboardButton('Додати'), types.KeyboardButton('Припинити'))
        bot.reply_to(
            message,
            "Ви можете додати ще одні піддони або завершити введення.",
            reply_markup=markup
        )


@bot.message_handler(func=lambda message: message.text.lower() in ['додати', 'припинити'])
def add_or_finish_pallets(message):
    if message.text.lower() == 'додати':
        bot.reply_to(message, "Яка кількість піддонів?")
        bot.register_next_step_handler(message, collect_pallet_amount)
    else:
        bot.reply_to(message, "Ось інформація про всі піддони:")
        for i, pallet in enumerate(pallets, start=1):
            bot.send_message(
                message.chat.id,
                (f"Піддон {i}:\n"
                 f"Контрагент: {pallet['counterpartie_name']}\n"
                 f"Кількість: {float(pallet['pallet_amount'])}\n"
                 f"Загальна вартість: {float(pallet['pallet_total_price'])}")
            )
        markup = types.ReplyKeyboardMarkup(one_time_keyboard=True)
        markup.add(types.KeyboardButton('Етрус'), types.KeyboardButton('Фастпол'))
        bot.reply_to(message, "Оберіть субсклад (Етрус або Фастпол):", reply_markup=markup)
        bot.register_next_step_handler(message, select_pallet_subwarehouse)


def select_pallet_subwarehouse(message):
    if message.text.lower() not in ['етрус', 'фастпол']:
        bot.reply_to(message, "Неправильний вибір. Оберіть субсклад: Етрус або Фастпол.")
        bot.register_next_step_handler(message, select_pallet_subwarehouse)
    else:
        global selected_subwarehouse_value
        selected_subwarehouse_value = message.text
        markup = types.ReplyKeyboardMarkup(one_time_keyboard=True)
        markup.add(types.KeyboardButton('Погодити'), types.KeyboardButton('Відмовити'))
        bot.reply_to(
            message,
            "Якщо все гаразд, натисніть 'Погодити', щоб надіслати інформацію",
            reply_markup=markup
        )
        bot.register_next_step_handler(message, confirm_pallets)


@bot.message_handler(func=lambda message: message.text.lower() in ['погодити', 'відмовити'])
def confirm_pallets(message):
    if message.text.lower() == 'погодити':
        counterpartie = counterparties_collection.find_one({'telegramID': message.from_user.id})
        data_to_save = {
            'pallets': pallets,
            'date': datetime.now().strftime('%Y-%m-%d'),
            'counterpartie_name': counterpartie['name'],
            'counterpartie_code': counterpartie['code'],
            'subwarehouse': selected_subwarehouse_value
        }
        pallets_collection.insert_one(data_to_save)
        bot.reply_to(message, "Інформація про піддони успішно надіслана")
        bot.send_message(message.chat.id, "Головне меню", reply_markup=main_menu_markup)
    else:
        bot.reply_to(message, "Операція скасована")
        bot.send_message(message.chat.id, "Головне меню", reply_markup=main_menu_markup)


# -------------------------- Main Polling Loop -------------------------- #
if __name__ == '__main__':
    while True:
        try:
            bot.polling(none_stop=True)
        except Exception:
            print(traceback.format_exc())

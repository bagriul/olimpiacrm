import os
import base64
import traceback
from io import BytesIO
import pymongo
import telebot
from telebot import types
import config
import xml.etree.ElementTree as ET
import requests
from uuid import uuid4
from datetime import datetime

# Define states for the conversation
SHOP_NAME, PRODUCT_NAME, PRODUCT_AMOUNT, PHOTO, CONFIRMATION = range(5)

# MongoDB setup
mongo_client = pymongo.MongoClient(config.MONGO_STRING)
db = mongo_client['olimpia_crm']
merchants_reports_collection = db['merchants_reports']
orders_collection = db['orders']
counterparties_collection = db['counterparties']
manufactured_products_collection = db['manufactured_products']
used_raw_collection = db['used_raw']
defective_products_collection = db['defective_products']
pallets_collection = db['pallets']

# Your Telegram Bot token
bot_token = config.bot_token
bot = telebot.TeleBot(bot_token)

# Main menu keyboard markup
main_menu_markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
main_menu_markup.add(types.KeyboardButton("Звіт мерчандайзера"))
main_menu_markup.add(types.KeyboardButton("Створити замовлення"))
main_menu_markup.add(types.KeyboardButton("Внести інформацію про кількість виробленої продукції"))
main_menu_markup.add(types.KeyboardButton('Внести інформацію про кількість використаної сировини'))
main_menu_markup.add(types.KeyboardButton('Бракована продукція'))
main_menu_markup.add(types.KeyboardButton('Піддони'))


def get_warehouse_data(warehouse_name):
    """Функція для отримання даних зі складу"""
    if warehouse_name == 'Етрус':
        url = 'https://olimpia.comp.lviv.ua:8189/BaseWeb/hs/base?action=getreportrest'
    elif warehouse_name == 'Фастпол':
        url = 'https://olimpia.comp.lviv.ua:8189/BaseWeb1/hs/base?action=getreportrest'
    else:
        raise ValueError(f"Невідомий склад: {warehouse_name}")

    response = requests.get(url, auth=('CRM', 'CegJr6YcK1sTnljgTIly'))

    if response.status_code != 200:
        raise Exception(f"Помилка запиту: {response.status_code}")

    # Парсимо XML відповідь
    xml_string = response.text
    return ET.fromstring(xml_string)


# Callback function for the /start command
@bot.message_handler(commands=['start'])
def start(message):
    markup = types.ReplyKeyboardMarkup(one_time_keyboard=True, resize_keyboard=True)
    markup.add(types.KeyboardButton('Надіслати номер телефону', request_contact=True))
    bot.send_message(message.chat.id, 'Поділіться, будь ласка, вашим номером телефону натиснувши кнопку нижче', reply_markup=markup)


@bot.message_handler(content_types=['contact'])
def handle_contact(message):
    phone_number = message.contact.phone_number
    counterpartie = counterparties_collection.find_one({"phone_number": phone_number})
    if counterpartie:
        counterparties_collection.find_one_and_update(counterpartie,
                                                      {'$set': {'telegramID': message.from_user.id}})
        bot.send_message(message.chat.id, "Головне меню", reply_markup=main_menu_markup)
    else:
        bot.send_message(message.chat.id, 'Контрагента не знайдено')


@bot.message_handler(commands=['cancel'])
def cancel_handel(message):
    counterpartie = counterparties_collection.find_one({"telegramID": message.from_user.id})
    if counterpartie:
        bot.send_message(message.chat.id, "Головне меню", reply_markup=main_menu_markup)
    else:
        bot.send_message(message.chat.id, 'Контрагента не знайдено')


# Змінні для зберігання всіх товарів
products = []

@bot.message_handler(func=lambda message: message.text == "Звіт мерчандайзера", content_types=['text'])
def handle_start_info_collection(message):
    global products
    products = []  # Очищення списку перед новим звітом
    bot.send_message(message.chat.id, "Яка назва торгової точки?")
    bot.register_next_step_handler(message, shop_name)

# Callback function to collect the shop name
def shop_name(message):
    if message.text == '/cancel':
        cancel_handel(message)
    else:
        global name_shop
        name_shop = message.text
        # Now ask for subwarehouse
        markup = types.ReplyKeyboardMarkup(one_time_keyboard=True)
        markup.add(types.KeyboardButton('Фастпол'), types.KeyboardButton('Етрус'))
        bot.reply_to(message, "Оберіть субсклад (Фастпол або Етрус):", reply_markup=markup)
        bot.register_next_step_handler(message, subwarehouse)

# New function to handle subwarehouse selection
def subwarehouse(message):
    if message.text == '/cancel':
        cancel_handel(message)
    elif message.text in ['Фастпол', 'Етрус']:
        global selected_subwarehouse
        selected_subwarehouse = message.text
        bot.reply_to(message, "Яка назва товару?")
        bot.register_next_step_handler(message, product_name)
    else:
        bot.reply_to(message, "Будь ласка, оберіть один із варіантів: Фастпол або Етрус")
        bot.register_next_step_handler(message, subwarehouse)

# Callback function to collect the product name
def product_name(message):
    if message.text == '/cancel':
        cancel_handel(message)
    else:
        global name_product
        name_product = message.text
        bot.reply_to(message, "Яка кількість товару?")
        bot.register_next_step_handler(message, product_amount)

def product_amount(message):
    if message.text == '/cancel':
        cancel_handel(message)
    else:
        global amount_product
        amount_product = message.text
        bot.reply_to(message, 'Яка вартість товару?')
        bot.register_next_step_handler(message, product_price)

def product_price(message):
    if message.text == '/cancel':
        cancel_handel(message)
    else:
        global price_product
        price_product = message.text
        bot.reply_to(message, 'Яка кількість акційного товару?')
        bot.register_next_step_handler(message, sale_amount)

def sale_amount(message):
    if message.text == '/cancel':
        cancel_handel(message)
    else:
        global amount_sale
        amount_sale = message.text
        bot.reply_to(message, 'Яка вартість акційного товару?')
        bot.register_next_step_handler(message, sale_price)

def sale_price(message):
    if message.text == '/cancel':
        cancel_handel(message)
    else:
        global price_sale
        price_sale = message.text
        bot.reply_to(message, "Надішліть фото, або повідомлення 'скасувати'")
        bot.register_next_step_handler(message, photo)

# Callback function to collect the optional photo
def photo(message):
    if message.text == '/cancel':
        cancel_handel(message)
    elif message.photo:
        # Take the first photo in the message
        photo = message.photo[-1].file_id
        photo_info = bot.get_file(photo)
        photo_file = bot.download_file(photo_info.file_path)
        photo_bytes = BytesIO(photo_file)
        global photo_base64
        photo_base64 = base64.b64encode(photo_bytes.read()).decode('utf-8')
        bot.reply_to(message, "Фото отримано та збережено!")
    else:
        photo_base64 = None
        bot.reply_to(message, "Фото не отримано")

    # Додаємо інформацію про товар у список
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
    bot.reply_to(message, "Ви можете додати ще один товар або завершити введення.", reply_markup=markup)

@bot.message_handler(func=lambda message: message.text.lower() in ['додати ще товар', 'завершити'])
def add_or_finish(message):
    if message.text.lower() == 'додати ще товар':
        bot.reply_to(message, "Яка назва товару?")
        bot.register_next_step_handler(message, product_name)
    else:
        # Підтвердження всіх товарів
        bot.reply_to(message, "Ось інформація про всі товари:")
        for i, product in enumerate(products, start=1):
            bot.send_message(message.chat.id, f"Товар {i}:\n"
                                              f"Назва: {product['product_name']}\n"
                                              f"Кількість: {float(product['product_amount'])}\n"
                                              f"Вартість: {float(product['product_price'])}\n"
                                              f"Кількість акційного товару: {float(product['sale_amount'])}\n"
                                              f"Вартість акційного товару: {float(product['sale_price'])}\n")

        markup = types.ReplyKeyboardMarkup(one_time_keyboard=True)
        markup.add(types.KeyboardButton('Підтвердити'), types.KeyboardButton('Скасувати'))
        bot.reply_to(message, "Якщо все гаразд, натисніть 'Підтвердити', щоб надіслати інформацію", reply_markup=markup)


@bot.message_handler(func=lambda message: message.text.lower() in ['підтвердити', 'скасувати'])
def confirmation(message):
    if message.text.lower() == 'підтвердити':
        counterpartie = counterparties_collection.find_one({'telegramID': message.from_user.id})

        # Calculate the total sum of product_price and sale_price
        total_sum = 0
        for product in products:
            product_price = float(product['product_price']) if product['product_price'] else 0
            sale_price = float(product['sale_price']) if product['sale_price'] else 0
            total_sum += (product_price * float(product['product_amount'])) + (sale_price * float(product['sale_amount']))

        # Prepare data to save, including the total sum
        data_to_save = {
            'shop_name': name_shop,
            'subwarehouse': selected_subwarehouse,  # Add subwarehouse to the saved data
            'products': products,
            'date': datetime.now().strftime('%Y-%m-%d'),
            'counterpartie_name': counterpartie['name'],
            'counterpartie_code': counterpartie['code'],
            'counterpartie_warehouse': counterpartie['warehouse'],
            'total_price_sum': total_sum  # Total sum field
        }

        # Save to MongoDB
        merchants_reports_collection.insert_one(data_to_save)
        bot.reply_to(message, "Інформація успішно надіслана")
        bot.send_message(message.chat.id, "Головне меню", reply_markup=main_menu_markup)
    else:
        bot.reply_to(message, "Операція скасована")
        bot.send_message(message.chat.id, "Головне меню", reply_markup=main_menu_markup)


user_data = {}


# Функція для вибору складу
@bot.message_handler(func=lambda message: message.text == "Створити замовлення", content_types=['text'])
def choose_warehouse(message):
    keyboard = types.InlineKeyboardMarkup()
    etrus_button = types.InlineKeyboardButton(text="Етрус", callback_data="warehouse_etrus")
    fastpol_button = types.InlineKeyboardButton(text="Фастпол", callback_data="warehouse_fastpol")
    keyboard.add(etrus_button, fastpol_button)
    bot.send_message(message.from_user.id, 'Оберіть склад:', reply_markup=keyboard)


# Обробник вибору складу і запит на продукти
@bot.callback_query_handler(func=lambda call: call.data.startswith('warehouse_'))
def ask_product(call):
    try:
        if call.data == "warehouse_etrus":
            warehouse_name = 'Етрус'
            warehouse_name_short = 'e'
        elif call.data == "warehouse_fastpol":
            warehouse_name = 'Фастпол'
            warehouse_name_short = 'f'
    except AttributeError:
        warehouse_name = user_data['product'][0]['subwarehouse']
        if warehouse_name == 'Етрус':
            warehouse_name_short = 'e'
        elif warehouse_name == 'Фастпол':
            warehouse_name_short = 'f'

    try:
        # Використання функції для запиту даних зі складу
        root = get_warehouse_data(warehouse_name)
    except Exception as e:
        bot.send_message(call.message.chat.id, f"Виникла помилка: {e}")
        return

    # Створення клавіатури для вибору продуктів
    keyboard = types.InlineKeyboardMarkup()
    for product in root.findall('Product'):
        code = product.get('Code')
        good = product.get('Good')
        type = product.get('Type')
        if type == '2':  # Фільтруємо продукти за типом
            button = types.InlineKeyboardButton(text=good, callback_data=f"orderproduct_{code}_{warehouse_name_short}")
            keyboard.add(button)

    try:
        bot.send_message(call.message.chat.id, 'Виберіть необхідний продукт', reply_markup=keyboard)
    except AttributeError:
        bot.send_message(call.from_user.id, 'Виберіть необхідний продукт', reply_markup=keyboard)

@bot.callback_query_handler(func=lambda call: call.data.startswith('orderproduct'))
def callback_inline(call):
    if call.data.split('_')[2] == 'f':
        subwarehouse = 'Фастпол'
    elif call.data.split('_')[2] == 'e':
        subwarehouse = 'Етрус'

    current_product = {'code': call.data.split('_')[1],
                       'subwarehouse': subwarehouse}
    msg = bot.send_message(call.from_user.id, "Введіть кількість товару:")
    bot.register_next_step_handler(msg, confirm_add_another_product, current_product)

def confirm_add_another_product(message, current_product):
    if message.text == '/cancel':
        cancel_handel(message)
    else:
        # Ініціалізуємо список продуктів, якщо його немає
        if 'product' not in user_data:
            user_data['product'] = []
        current_product['amount'] = int(message.text)
        user_data['product'].append(current_product)
        msg = bot.send_message(message.chat.id, "Додати ще один продукт? (так/ні)")
        bot.register_next_step_handler(msg, check_add_more)

def check_add_more(message):
    if message.text == '/cancel':
        cancel_handel(message)
    elif message.text.lower() == 'так':
        ask_product(message)
    else:
        msg = bot.send_message(message.from_user.id, 'Введіть коментар до замовлення')
        bot.register_next_step_handler(msg, ask_comment)

def ask_comment(message):
    if message.text == '/cancel':
        cancel_handel(message)
    else:
        user_data['comment'] = message.text
        save_data_to_mongo(message)

def save_data_to_mongo(message):
    user_data['order_number'] = str(uuid4())
    user_data['date'] = str(datetime.now())
    counterpartie = counterparties_collection.find_one({'telegramID': message.from_user.id})
    user_data['counterpartie_code'] = counterpartie['code']
    user_data['status'] = {'name': 'Прийнято', 'colour': '#28BEFF'}
    user_data['payment_status'] = None

    if user_data["product"][0]["subwarehouse"] == 'Етрус':
        base_web = 'BaseWeb'
    elif user_data["product"][0]["subwarehouse"] == 'Фастпол':
        base_web = 'BaseWeb1'

    user_data_without_status = user_data.copy()
    user_data_without_status.pop('status')
    request = requests.post(f'https://olimpia.comp.lviv.ua:8189/{base_web}/hs/base?action=CreateOrder',
                            json=user_data_without_status, auth=('CRM', 'CegJr6YcK1sTnljgTIly'))
    root = ET.fromstring(request.text)
    answer = root.find('Answer').text
    order = root.find('order').text

    if answer == 'ok':
        user_data['order_number_1c'] = order
        orders_collection.insert_one(user_data.copy())
        bot.send_message(message.chat.id, "Дані успішно надіслано!")
    else:
        bot.send_message(message.from_user.id, 'Помилка надсилання даних')


# Функція для вибору складу
@bot.message_handler(func=lambda message: message.text == "Внести інформацію про кількість виробленої продукції", content_types=['text'])
def choose_warehouse_for_manufactured(message):
    keyboard = types.InlineKeyboardMarkup()
    etrus_button = types.InlineKeyboardButton(text="Етрус", callback_data="manufactured_warehouse_etrus")
    fastpol_button = types.InlineKeyboardButton(text="Фастпол", callback_data="manufactured_warehouse_fastpol")
    keyboard.add(etrus_button, fastpol_button)
    bot.send_message(message.from_user.id, 'Оберіть склад:', reply_markup=keyboard)

# Обробник вибору складу і запит на продукти для виробленої продукції
@bot.callback_query_handler(func=lambda call: call.data.startswith('manufactured_warehouse_'))
def ask_manufactured(call):
    global user_data
    user_data = {'product': []}

    # Визначаємо склад на основі вибору користувача
    if call.data == "manufactured_warehouse_etrus":
        warehouse_name = 'Етрус'
        warehouse_name_short = 'e'
    elif call.data == "manufactured_warehouse_fastpol":
        warehouse_name = 'Фастпол'
        warehouse_name_short = 'f'

    try:
        # Використання універсальної функції для запиту даних зі складу
        root = get_warehouse_data(warehouse_name)
    except Exception as e:
        bot.send_message(call.message.chat.id, f"Виникла помилка: {e}")
        return

    # Створюємо клавіатуру для вибору продуктів
    keyboard = types.InlineKeyboardMarkup()
    for product in root.findall('Product'):
        code = product.get('Code')
        good = product.get('Good')
        type = product.get('Type')
        if type == '2':  # Фільтруємо продукти за типом
            button = types.InlineKeyboardButton(text=good, callback_data=f"mp_{code}_{warehouse_name_short}")
            keyboard.add(button)

    bot.send_message(call.message.chat.id, 'Виберіть необхідний продукт для внесення інформації про вироблену кількість', reply_markup=keyboard)


@bot.callback_query_handler(func=lambda call: call.data.startswith('mp_'))
def ask_amount(call):
    code = call.data.split('_')[1]
    warehouse_name_short = call.data.split('_')[2]

    if warehouse_name_short == 'e':
        warehouse_name = 'Етрус'
        url = 'https://olimpia.comp.lviv.ua:8189/BaseWeb/hs/base?action=getreportrest'
    elif warehouse_name_short == 'f':
        warehouse_name = 'Фастпол'
        url = 'https://olimpia.comp.lviv.ua:8189/BaseWeb1/hs/base?action=getreportrest'
    response = requests.get(url, auth=('CRM', 'CegJr6YcK1sTnljgTIly'))
    xml_string = response.text
    root = ET.fromstring(xml_string)
    for product in root.findall('Product'):
        if product.get('Code') == code:
            good = product.get('Good')

    msg = bot.send_message(call.from_user.id, 'Введіть кількість виготовленої продукції')
    bot.register_next_step_handler(msg, confirm_manufactured, code, good, warehouse_name)


def confirm_manufactured(message, code, good, warehouse_name):
    if message.text == '/cancel':
        cancel_handel(message)
    else:
        amount = int(message.text)
        user_data['product'].append({'code': code, 'amount': amount})

        counterpartie = counterparties_collection.find_one({'telegramID': message.from_user.id})
        if warehouse_name == 'Етрус':
            base_web = 'BaseWeb'
        elif warehouse_name == 'Фастпол':
            base_web = 'BaseWeb1'

        request = requests.post(f'https://olimpia.comp.lviv.ua:8189/{base_web}/hs/base?action=CreateProduction',
                                json=user_data, auth=('CRM', 'CegJr6YcK1sTnljgTIly'))
        root = ET.fromstring(request.text)
        answer = root.find('Answer').text
        production = root.find('production').text

        if answer == 'ok':
            manufactured_products_collection.insert_one({'date': datetime.now(),
                                                         'document': production,
                                                         'subwarehouse': warehouse_name,
                                                         'code': code,
                                                         'good': good,
                                                         'amount': amount})
            bot.send_message(message.chat.id, "Дані успішно надіслано!")
        else:
            bot.send_message(message.from_user.id, 'Помилка надсилання даних')


# Функція для вибору складу
@bot.message_handler(func=lambda message: message.text == "Внести інформацію про кількість використаної сировини", content_types=['text'])
def choose_warehouse_for_raw_materials(message):
    keyboard = types.InlineKeyboardMarkup()
    etrus_button = types.InlineKeyboardButton(text="Етрус", callback_data="raw_warehouse_etrus")
    fastpol_button = types.InlineKeyboardButton(text="Фастпол", callback_data="raw_warehouse_fastpol")
    keyboard.add(etrus_button, fastpol_button)
    bot.send_message(message.from_user.id, 'Оберіть склад:', reply_markup=keyboard)

# Обробник вибору складу і запит на продукти для сировини
@bot.callback_query_handler(func=lambda call: call.data.startswith('raw_warehouse_'))
def ask_used_raw_materials(call):
    global user_data
    user_data = {'product': []}

    # Визначаємо склад на основі вибору користувача
    if call.data == "raw_warehouse_etrus":
        warehouse_name = 'Етрус'
        warehouse_name_short = 'e'
    elif call.data == "raw_warehouse_fastpol":
        warehouse_name = 'Фастпол'
        warehouse_name_short = 'f'

    try:
        # Використання універсальної функції для запиту даних зі складу
        root = get_warehouse_data(warehouse_name)
    except Exception as e:
        bot.send_message(call.message.chat.id, f"Виникла помилка: {e}")
        return

    # Створюємо клавіатуру для вибору сировини (type == '1' для сировини)
    keyboard = types.InlineKeyboardMarkup()
    for product in root.findall('Product'):
        code = product.get('Code')
        good = product.get('Good')
        type = product.get('Type')
        if type == '1':  # Фільтруємо продукти за типом сировини
            button = types.InlineKeyboardButton(text=good, callback_data=f"usedraw_{code}_{warehouse_name_short}")
            keyboard.add(button)

    bot.send_message(call.message.chat.id, 'Виберіть необхідну сировину для внесення інформації про використання', reply_markup=keyboard)


@bot.callback_query_handler(func=lambda call: call.data.startswith('usedraw'))
def ask_used(call):
    raw_code = call.data.split('_')[1]
    warehouse_name_short = call.data.split('_')[2]

    msg = bot.send_message(call.from_user.id, 'Введіть кількість сировини, яка пішла у виробництво')
    bot.register_next_step_handler(msg, confirm_used, raw_code, warehouse_name_short)


def confirm_used(message, raw_code, warehouse_name_short):
    if message.text == '/cancel':
        cancel_handel(message)
    else:
        used_amount = int(message.text)

        msg = bot.send_message(message.from_user.id, 'Введіть кількість браку')
        bot.register_next_step_handler(msg, ask_defect, used_amount, raw_code, warehouse_name_short)


def ask_defect(message, used_amount, raw_code, warehouse_name_short):
    if message.text == '/cancel':
        cancel_handel(message)
    else:
        defect_amount = int(message.text)

        if warehouse_name_short == 'e':
            warehouse_name = 'Етрус'
            url = 'https://olimpia.comp.lviv.ua:8189/BaseWeb/hs/base?action=getreportrest'
        elif warehouse_name_short == 'f':
            warehouse_name = "Фастпол"
            url = 'https://olimpia.comp.lviv.ua:8189/BaseWeb1/hs/base?action=getreportrest'
        response = requests.get(url, auth=('CRM', 'CegJr6YcK1sTnljgTIly'))
        xml_string = response.text
        root = ET.fromstring(xml_string)
        keyboard = types.InlineKeyboardMarkup()
        for product in root.findall('Product'):
            code = product.get('Code')
            good = product.get('Good')
            type = product.get('Type')
            if type == '1' and code == raw_code:
                raw_name = good

        used_raw_collection.insert_one({'date': datetime.now(),
                                        'code': raw_code,
                                        'good': raw_name,
                                        'amount': used_amount,
                                        'defect': defect_amount,
                                        'subwarehouse': warehouse_name})
        bot.send_message(message.from_user.id, 'Дані успішно надіслано')


# Змінні для зберігання всієї бракованої продукції
defective_products = []


@bot.message_handler(func=lambda message: message.text == "Бракована продукція", content_types=['text'])
def handle_defective_product_collection(message):
    global defective_products
    defective_products = []  # Очищення списку перед новим звітом
    bot.send_message(message.chat.id, "Яка назва продукту?")
    bot.register_next_step_handler(message, defective_product_name, {})


def defective_product_name(message, product_data):
    if message.text == '/cancel':
        cancel_handel(message)
    else:
        product_data['product_name'] = message.text
        bot.reply_to(message, "Яка дата повернення? (У форматі рік-місяць-день)")
        bot.register_next_step_handler(message, return_date, product_data)


def return_date(message, product_data):
    if message.text == '/cancel':
        cancel_handel(message)
    else:
        product_data['return_date'] = message.text
        bot.reply_to(message, "Яка кількість?")
        bot.register_next_step_handler(message, defective_product_amount, product_data)


def defective_product_amount(message, product_data):
    if message.text == '/cancel':
        cancel_handel(message)
    else:
        product_data['amount'] = message.text
        bot.reply_to(message, "Яка загальна вартість?")
        bot.register_next_step_handler(message, defective_product_price, product_data)


def defective_product_price(message, product_data):
    if message.text == '/cancel':
        cancel_handel(message)
    else:
        product_data['total_price'] = message.text
        defective_products.append(product_data)

        markup = types.ReplyKeyboardMarkup(one_time_keyboard=True)
        markup.add(types.KeyboardButton('Додати ще'), types.KeyboardButton('Закінчити'))
        bot.reply_to(message, "Ви можете додати ще одну браковану продукцію або завершити введення.",
                     reply_markup=markup)


@bot.message_handler(func=lambda message: message.text.lower() in ['додати ще', 'закінчити'])
def add_or_finish_defective(message):
    if message.text.lower() == 'додати ще':
        product_data = {}  # Create a new dictionary for the new product
        bot.reply_to(message, "Яка назва продукту?")
        bot.register_next_step_handler(message, defective_product_name, product_data)
    else:
        # Підтвердження всіх бракованих продуктів
        bot.reply_to(message, "Ось інформація про всі браковані продукти:")
        for i, defective_product in enumerate(defective_products, start=1):
            bot.send_message(message.chat.id, f"Продукт {i}:\n"
                                              f"Назва: {defective_product['product_name']}\n"
                                              f"Дата повернення: {defective_product['return_date']}\n"
                                              f"Кількість: {float(defective_product['amount'])}\n"
                                              f"Загальна вартість: {float(defective_product['total_price'])}\n")

        # Ask for subwarehouse selection
        markup = types.ReplyKeyboardMarkup(one_time_keyboard=True)
        markup.add(types.KeyboardButton('Фастпол'), types.KeyboardButton('Етрус'))
        bot.reply_to(message, "Оберіть субсклад (Фастпол або Етрус):", reply_markup=markup)
        bot.register_next_step_handler(message, confirmation_defective)


@bot.message_handler(func=lambda message: message.text.lower() in ['фастпол', 'етрус'])
def confirmation_defective(message):
    selected_subwarehouse = message.text  # Store the selected subwarehouse
    counterpartie = counterparties_collection.find_one({'telegramID': message.from_user.id})

    # Зберігаємо дані до MongoDB
    data_to_save = {
        'defective_products': defective_products,
        'date': datetime.now().strftime('%Y-%m-%d'),
        'counterpartie_name': counterpartie['name'],
        'counterpartie_code': counterpartie['code'],
        'subwarehouse': selected_subwarehouse  # Add subwarehouse here
    }
    defective_products_collection.insert_one(data_to_save)
    bot.reply_to(message, "Інформація про браковану продукцію успішно надіслана")
    bot.send_message(message.chat.id, "Головне меню", reply_markup=main_menu_markup)


# Змінні для зберігання всіх піддонів
pallets = []


@bot.message_handler(func=lambda message: message.text == "Піддони", content_types=['text'])
def handle_pallet_collection(message):
    global pallets
    pallets = []  # Очищення списку перед новим звітом

    # Автоматичне завантаження контрагента з бази даних
    counterpartie = counterparties_collection.find_one({'telegramID': message.from_user.id})
    if not counterpartie:
        bot.reply_to(message, "Контрагент не знайдений. Спробуйте ще раз або зверніться до підтримки.")
        return

    global counterpartie_name
    counterpartie_name = counterpartie['name']

    bot.send_message(message.chat.id, "Яка кількість піддонів?")
    bot.register_next_step_handler(message, collect_pallet_amount)


# Callback function to collect the pallet amount
def collect_pallet_amount(message):
    if message.text == '/cancel':
        cancel_handel(message)
    else:
        global pallet_amount_value  # Renamed to avoid conflict
        pallet_amount_value = message.text
        bot.reply_to(message, "Яка загальна вартість піддонів?")
        bot.register_next_step_handler(message, collect_pallet_total_price)


# Callback function to collect the pallet total price
def collect_pallet_total_price(message):
    if message.text == '/cancel':
        cancel_handel(message)
    else:
        global pallet_total_price_value  # Renamed to avoid conflict
        pallet_total_price_value = message.text

        # Додаємо інформацію про піддони у список
        pallets.append({
            'counterpartie_name': counterpartie_name,
            'pallet_amount': pallet_amount_value,
            'pallet_total_price': pallet_total_price_value
        })

        markup = types.ReplyKeyboardMarkup(one_time_keyboard=True)
        markup.add(types.KeyboardButton('Додати'), types.KeyboardButton('Припинити'))
        bot.reply_to(message, "Ви можете додати ще одні піддони або завершити введення.", reply_markup=markup)


@bot.message_handler(func=lambda message: message.text.lower() in ['додати', 'припинити'])
def add_or_finish_pallets(message):
    if message.text.lower() == 'додати':
        bot.reply_to(message, "Яка кількість піддонів?")
        bot.register_next_step_handler(message, collect_pallet_amount)
    else:
        # Підтвердження всіх піддонів
        bot.reply_to(message, "Ось інформація про всі піддони:")
        for i, pallet in enumerate(pallets, start=1):
            bot.send_message(message.chat.id, f"Піддон {i}:\n"
                                              f"Контрагент: {pallet['counterpartie_name']}\n"
                                              f"Кількість: {float(pallet['pallet_amount'])}\n"
                                              f"Загальна вартість: {float(pallet['pallet_total_price'])}\n")

        # Ask for subwarehouse selection
        markup = types.ReplyKeyboardMarkup(one_time_keyboard=True)
        markup.add(types.KeyboardButton('Етрус'), types.KeyboardButton('Фастпол'))
        bot.reply_to(message, "Оберіть субсклад (Етрус або Фастпол):", reply_markup=markup)
        bot.register_next_step_handler(message, select_subwarehouse)


# This function handles subwarehouse selection
def select_subwarehouse(message):
    if message.text.lower() not in ['етрус', 'фастпол']:
        bot.reply_to(message, "Неправильний вибір. Оберіть субсклад: Етрус або Фастпол.")
        bot.register_next_step_handler(message, select_subwarehouse)
    else:
        global selected_subwarehouse_value  # Renamed to avoid conflict
        selected_subwarehouse_value = message.text  # Store the selected subwarehouse

        # Proceed to confirmation
        markup = types.ReplyKeyboardMarkup(one_time_keyboard=True)
        markup.add(types.KeyboardButton('Погодити'), types.KeyboardButton('Відмовити'))
        bot.reply_to(message, "Якщо все гаразд, натисніть 'Погодити', щоб надіслати інформацію", reply_markup=markup)
        bot.register_next_step_handler(message, confirmation_pallets)


# This function handles the final confirmation
@bot.message_handler(func=lambda message: message.text.lower() in ['погодити', 'відмовити'])
def confirmation_pallets(message):
    if message.text.lower() == 'погодити':
        counterpartie = counterparties_collection.find_one({'telegramID': message.from_user.id})

        # Зберігаємо дані до MongoDB
        data_to_save = {
            'pallets': pallets,
            'date': datetime.now().strftime('%Y-%m-%d'),
            'counterpartie_name': counterpartie['name'],
            'counterpartie_code': counterpartie['code'],
            'subwarehouse': selected_subwarehouse_value  # Use renamed variable
        }
        pallets_collection.insert_one(data_to_save)
        bot.reply_to(message, "Інформація про піддони успішно надіслана")
        bot.send_message(message.chat.id, "Головне меню", reply_markup=main_menu_markup)
    else:
        bot.reply_to(message, "Операція скасована")
        bot.send_message(message.chat.id, "Головне меню", reply_markup=main_menu_markup)


if __name__ == '__main__':
    while True:
        try:
            bot.polling()
        except:
            print(traceback.format_exc())

import os
import base64
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

# Your Telegram Bot token
bot_token = config.bot_token
bot = telebot.TeleBot(bot_token)

# Main menu keyboard markup
main_menu_markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
main_menu_markup.add(types.KeyboardButton("Звіт мерчандайзера"))
main_menu_markup.add(types.KeyboardButton("Створити замовлення"))
main_menu_markup.add(types.KeyboardButton("Внести інформацію про кількість виробленої продукції"))
main_menu_markup.add(types.KeyboardButton('Внести інформацію про кількість використаної сировини'))


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


@bot.message_handler(func=lambda message: message.text == "Звіт мерчандайзера", content_types=['text'])
def handle_start_info_collection(message):
    bot.send_message(message.chat.id, "Яка назва торгової точки?")
    bot.register_next_step_handler(message, shop_name)

# Callback function to collect the shop name
def shop_name(message):
    global name_shop
    name_shop = message.text
    bot.reply_to(message, f"Яка назва товару?")
    bot.register_next_step_handler(message, product_name)

# Callback function to collect the product name
def product_name(message):
    global name_product
    name_product = message.text
    bot.reply_to(message, f"Яка кількість товару?")
    bot.register_next_step_handler(message, product_amount)


def product_amount(message):
    global amount_product
    amount_product = message.text
    bot.reply_to(message, 'Яка вартість товару?')
    bot.register_next_step_handler(message, product_price)


def product_price(message):
    global price_product
    price_product = message.text
    bot.reply_to(message, 'Яка кількість акційного товару?')
    bot.register_next_step_handler(message, sale_amount)


def sale_amount(message):
    global amount_sale
    amount_sale = message.text
    bot.reply_to(message, 'Яка вартість акційного товару?')
    bot.register_next_step_handler(message, sale_price)


def sale_price(message):
    global price_sale
    price_sale = message.text
    bot.reply_to(message, f"Надішліть фото, або повідомлення 'скасувати'")
    bot.register_next_step_handler(message, photo)


# Callback function to collect the optional photo
def photo(message):
    if message.photo:
        # Take the first photo in the message
        photo = message.photo[-1].file_id
        photo_info = bot.get_file(photo)
        photo_file = bot.download_file(photo_info.file_path)
        photo_bytes = BytesIO(photo_file)
        global photo_base64
        photo_base64 = base64.b64encode(photo_bytes.read()).decode('utf-8')
        message.photo = photo_base64
        bot.reply_to(message, "Фото отримано та збережено!")
    else:
        photo_base64 = None
        bot.reply_to(message, "Фото не отримано")

    markup = types.ReplyKeyboardMarkup(one_time_keyboard=True)
    markup.add(types.KeyboardButton('Підтвердити'), types.KeyboardButton('Скасувати'))
    bot.reply_to(message, "Ось інформація яку ви надали:\n"
                          f"Назва торгової точки: {name_shop}\n"
                          f"Назва товару: {name_product}\n"
                          f"Кількість товару: {float(amount_product)}\n"
                          f"Вартість товару: {float(price_product)}\n"
                          f"Кількість акційного товару: {float(amount_sale)}\n"
                          f"Вартість акційного товару: {float(price_sale)}\n"
                          "Якщо все гаразд, то натисніть 'Підтвердити', щоб надіслати інформацію", reply_markup=markup)


@bot.message_handler(func=lambda message: message.text.lower() in ['підтвердити', 'скасувати'])
def confirmation(message):
    if message.text.lower() == 'підтвердити':
        counterpartie = counterparties_collection.find_one({'telegramID': message.from_user.id})
        # Save data to MongoDB
        data_to_save = {
            'shop_name': name_shop,
            'product_name': name_product,
            'product_amount': amount_product,
            'product_price': price_product,
            'sale_amount': amount_sale,
            'sale_price': price_sale,
            'date': datetime.now().strftime('%Y-%m-%d'),
            'counterpartie_name': counterpartie['name'],
            'counterpartie_code': counterpartie['code'],
            'photo': [photo_base64]
        }
        merchants_reports_collection.insert_one(data_to_save)
        bot.reply_to(message, "Інформація успішно надіслана")
        bot.send_message(message.chat.id, "Головне меню", reply_markup=main_menu_markup)
    else:
        bot.reply_to(message, "Операція скасована")
        bot.send_message(message.chat.id, "Головне меню", reply_markup=main_menu_markup)


user_data = {}


@bot.message_handler(func=lambda message: message.text == "Створити замовлення", content_types=['text'])
def ask_product(message):
    global user_data
    user_data = {'product': []}

    counterpartie = counterparties_collection.find_one({'telegramID': message.from_user.id})
    if counterpartie['warehouse'] == 'Етрус':
        url = 'https://olimpia.comp.lviv.ua:8189/BaseWeb/hs/base?action=getreportrest'
    elif counterpartie['warehouse'] == 'Фастпол':
        url = 'https://olimpia.comp.lviv.ua:8189/BaseWeb1/hs/base?action=getreportrest'

    response = requests.get(url, auth=('CRM', 'CegJr6YcK1sTnljgTIly'))
    xml_string = response.text
    root = ET.fromstring(xml_string)
    keyboard = types.InlineKeyboardMarkup()
    for product in root.findall('Product'):
        code = product.get('Code')
        good = product.get('Good')
        type = product.get('Type')
        if type == '2':
            button = types.InlineKeyboardButton(text=good, callback_data=f"orderproduct_{code}")
            keyboard.add(button)
    bot.send_message(message.from_user.id, 'Виберіть необхідний продукт', reply_markup=keyboard)

@bot.callback_query_handler(func=lambda call: call.data.startswith('orderproduct'))
def callback_inline(call):
    current_product = {'code': call.data.split('_')[1]}
    msg = bot.send_message(call.from_user.id, "Введіть кількість товару:")
    bot.register_next_step_handler(msg, confirm_add_another_product, current_product)

def confirm_add_another_product(message, current_product):
    current_product['amount'] = int(message.text)
    user_data['product'].append(current_product)
    msg = bot.send_message(message.chat.id, "Додати ще один продукт? (так/ні)")
    bot.register_next_step_handler(msg, check_add_more)

def check_add_more(message):
    if message.text.lower() == 'так':
        ask_product(message)
    else:
        msg = bot.send_message(message.from_user.id, 'Введіть коментар до замовлення')
        bot.register_next_step_handler(msg, ask_comment)

def ask_comment(message):
    user_data['comment'] = message.text
    save_data_to_mongo(message)

def save_data_to_mongo(message):
    user_data['order_number'] = str(uuid4())
    user_data['date'] = str(datetime.now())
    counterpartie = counterparties_collection.find_one({'telegramID': message.from_user.id})
    user_data['counterpartie_code'] = counterpartie['code']
    user_data['status'] = {'name': 'Прийнято', 'colour': '#28BEFF'}

    counterpartie = counterparties_collection.find_one({'telegramID': message.from_user.id})
    if counterpartie['warehouse'] == 'Етрус':
        base_web = 'BaseWeb'
    elif counterpartie['warehouse'] == 'Фастпол':
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


@bot.message_handler(func=lambda message: message.text == "Внести інформацію про кількість виробленої продукції", content_types=['text'])
def ask_manufactured(message):
    global user_data
    user_data = {'product': []}

    counterpartie = counterparties_collection.find_one({'telegramID': message.from_user.id})
    if counterpartie['warehouse'] == 'Етрус':
        url = 'https://olimpia.comp.lviv.ua:8189/BaseWeb/hs/base?action=getreportrest'
    elif counterpartie['warehouse'] == 'Фастпол':
        url = 'https://olimpia.comp.lviv.ua:8189/BaseWeb1/hs/base?action=getreportrest'

    response = requests.get(url, auth=('CRM', 'CegJr6YcK1sTnljgTIly'))
    xml_string = response.text
    root = ET.fromstring(xml_string)
    keyboard = types.InlineKeyboardMarkup()
    for product in root.findall('Product'):
        code = product.get('Code')
        good = product.get('Good')
        type = product.get('Type')
        if type == '2':
            button = types.InlineKeyboardButton(text=good, callback_data=f"manufacturedproduct_{code}")
            keyboard.add(button)
    bot.send_message(message.from_user.id, 'Виберіть необхідний продукт', reply_markup=keyboard)


@bot.callback_query_handler(func=lambda call: call.data.startswith('manufacturedproduct'))
def ask_amount(call):
    code = call.data.split('_')[1]

    counterpartie = counterparties_collection.find_one({'telegramID': call.from_user.id})
    if counterpartie['warehouse'] == 'Етрус':
        url = 'https://olimpia.comp.lviv.ua:8189/BaseWeb/hs/base?action=getreportrest'
    elif counterpartie['warehouse'] == 'Фастпол':
        url = 'https://olimpia.comp.lviv.ua:8189/BaseWeb1/hs/base?action=getreportrest'
    response = requests.get(url, auth=('CRM', 'CegJr6YcK1sTnljgTIly'))
    xml_string = response.text
    root = ET.fromstring(xml_string)
    for product in root.findall('Product'):
        if product.get('Code') == code:
            good = product.get('Good')

    msg = bot.send_message(call.from_user.id, 'Введіть кількість виготовленої продукції')
    bot.register_next_step_handler(msg, confirm_manufactured, code, good)


def confirm_manufactured(message, code, good):
    amount = int(message.text)
    user_data['product'].append({'code': code, 'amount': amount})

    counterpartie = counterparties_collection.find_one({'telegramID': message.from_user.id})
    if counterpartie['warehouse'] == 'Етрус':
        base_web = 'BaseWeb'
    elif counterpartie['warehouse'] == 'Фастпол':
        base_web = 'BaseWeb1'

    request = requests.post(f'https://olimpia.comp.lviv.ua:8189/{base_web}/hs/base?action=CreateProduction',
                            json=user_data, auth=('CRM', 'CegJr6YcK1sTnljgTIly'))
    root = ET.fromstring(request.text)
    answer = root.find('Answer').text
    production = root.find('production').text

    if answer == 'ok':
        manufactured_products_collection.insert_one({'date': datetime.now(),
                                                     'document': production,
                                                     'code': code,
                                                     'good': good,
                                                     'amount': amount})
        bot.send_message(message.chat.id, "Дані успішно надіслано!")
    else:
        bot.send_message(message.from_user.id, 'Помилка надсилання даних')


@bot.message_handler(func=lambda message: message.text == "Внести інформацію про кількість використаної сировини", content_types=['text'])
def ask_manufactured(message):
    global user_data
    user_data = {'product': []}

    counterpartie = counterparties_collection.find_one({'telegramID': message.from_user.id})
    if counterpartie['warehouse'] == 'Етрус':
        url = 'https://olimpia.comp.lviv.ua:8189/BaseWeb/hs/base?action=getreportrest'
    elif counterpartie['warehouse'] == 'Фастпол':
        url = 'https://olimpia.comp.lviv.ua:8189/BaseWeb1/hs/base?action=getreportrest'

    response = requests.get(url, auth=('CRM', 'CegJr6YcK1sTnljgTIly'))
    xml_string = response.text
    root = ET.fromstring(xml_string)
    keyboard = types.InlineKeyboardMarkup()
    for product in root.findall('Product'):
        code = product.get('Code')
        good = product.get('Good')
        type = product.get('Type')
        if type == '1':
            button = types.InlineKeyboardButton(text=good, callback_data=f"usedraw_{code}")
            keyboard.add(button)
    bot.send_message(message.from_user.id, 'Виберіть необхідний продукт', reply_markup=keyboard)


@bot.callback_query_handler(func=lambda call: call.data.startswith('usedraw'))
def ask_used(call):
    raw_code = call.data.split('_')[1]

    msg = bot.send_message(call.from_user.id, 'Введіть кількість сировини, яка пішла у виробництво')
    bot.register_next_step_handler(msg, confirm_used, raw_code)


def confirm_used(message, raw_code):
    used_amount = int(message.text)

    msg = bot.send_message(message.from_user.id, 'Введіть кількість браку')
    bot.register_next_step_handler(msg, ask_defect, used_amount, raw_code)


def ask_defect(message, used_amount, raw_code):
    defect_amount = int(message.text)

    counterpartie = counterparties_collection.find_one({'telegramID': message.from_user.id})
    if counterpartie['warehouse'] == 'Етрус':
        url = 'https://olimpia.comp.lviv.ua:8189/BaseWeb/hs/base?action=getreportrest'
    elif counterpartie['warehouse'] == 'Фастпол':
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

    used_raw_collection.insert_one({'code': raw_code,
                                    'name': raw_name,
                                    'used': used_amount,
                                    'defect': defect_amount,
                                    'date': datetime.now()})
    bot.send_message(message.from_user.id, 'Дані успішно надіслано')


if __name__ == '__main__':
    bot.polling()

import os
import base64
from io import BytesIO
import pymongo
import telebot
from telebot import types
import config

# Define states for the conversation
SHOP_NAME, PRODUCT_NAME, PRODUCT_AMOUNT, PHOTO, CONFIRMATION = range(5)

# MongoDB setup
mongo_client = pymongo.MongoClient(config.MONGO_STRING)
db = mongo_client['olimpia_crm']
collection = db['merchants_reports']

# Your Telegram Bot token
bot_token = config.bot_token
bot = telebot.TeleBot(bot_token)

# Main menu keyboard markup
main_menu_markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
main_menu_markup.add(types.KeyboardButton("Звіт мерчандайзера"))

# Callback function for the /start command
@bot.message_handler(commands=['start'])
def start(message):
    bot.send_message(message.chat.id, "Головне меню", reply_markup=main_menu_markup)

# Callback function to handle the main menu button
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

# Callback function to collect the product amount
def product_amount(message):
    global amount_product
    amount_product = message.text
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
                          f"Кількість товару: {amount_product}\n"
                          "Якщо все гаразд, то натисніть 'Підтвердити', щоб надіслати інформацію", reply_markup=markup)

@bot.message_handler(func=lambda message: message.text.lower() in ['підтвердити', 'скасувати'])
def confirmation(message):
    if message.text.lower() == 'підтвердити':
        # Save data to MongoDB
        data_to_save = {
            'shop_name': name_shop,
            'product_name': name_product,
            'product_amount': amount_product,
            'photo': photo_base64
        }
        collection.insert_one(data_to_save)
        bot.reply_to(message, "Інформація успішно надіслана")
        bot.send_message(message.chat.id, "Головне меню", reply_markup=main_menu_markup)
    else:
        bot.reply_to(message, "Операція скасована")
        bot.send_message(message.chat.id, "Головне меню", reply_markup=main_menu_markup)

if __name__ == '__main__':
    bot.polling()

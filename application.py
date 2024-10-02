import pymongo

import config
from flask import Flask, request, Response, jsonify
from flask_cors import CORS
from flask_mail import Mail, Message
from pymongo import MongoClient
import jwt
from bson import json_util, ObjectId
from flask_bcrypt import Bcrypt
from datetime import datetime, timedelta
import json
import math
from gridfs import GridFS
import re
import boto3
from botocore.client import Config
import io
from botocore.exceptions import ClientError
import uuid
import requests
import xml.etree.ElementTree as ET
from uuid import uuid4
from analytics_functions import (
    total_sales, average_order_amount, order_volume_dynamic, paid_orders_percentage,
    analyze_repeat_orders, calculate_sales_agent_rating, calculate_product_rating,
    get_total_rest_by_warehouse, get_total_price_for_workwear, get_low_stock_products,
    get_products_with_expired_series, get_total_amount_for_distributor,
    get_total_amount_manufactured_by_good, get_total_used_raw, get_defect_raw_percentage,
    get_contracts_stats, sale_products_report, defective_products_report, pallets_report
)

application = Flask(__name__)

# Flask-Mail configuration for Gmail
application.config['MAIL_SERVER'] = 'smtp.gmail.com'
application.config['MAIL_PORT'] = 587
application.config['MAIL_USERNAME'] = 'your_email@gmail.com'  # Replace with your Gmail address
application.config['MAIL_PASSWORD'] = 'your_email_password'    # Replace with your Gmail password
application.config['MAIL_USE_TLS'] = True
application.config['MAIL_USE_SSL'] = False
application.config['MAIL_DEFAULT_SENDER'] = 'your_email@gmail.com'  # Replace with your Gmail address
mail = Mail(application)

CORS(application)
application.config['SECRET_KEY'] = config.SECRET_KEY
SECRET_KEY = config.SECRET_KEY
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

bcrypt = Bcrypt(application)


@application.route('/', methods=['GET'])
def test():
    return 'OlimpiaCRM API v1.0'


def decode_access_token(access_token, secret_key):
    try:
        payload = jwt.decode(access_token, secret_key, algorithms=['HS256'])
        print(payload)
        return payload
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None


def decode_refresh_token(refresh_token, secret_key):
    try:
        payload = jwt.decode(refresh_token, secret_key, algorithms=['HS256'])
        return payload
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None


def verify_access_token(access_token):
    try:
        decoded_token = decode_access_token(access_token, SECRET_KEY)
        if decoded_token:
            user_id = decoded_token.get('user_id')
            # Fetch user data from the database using the user_id
            user = users_collection.find_one({'_id': ObjectId(user_id)})
            if user:
                name = user['name']
                return jsonify({'name': name}), 200
            else:
                return jsonify({'message': 'User not found'}), 404
    except jwt.ExpiredSignatureError:
        return False
    except jwt.InvalidTokenError:
        return False


def verify_refresh_token(refresh_token):
    try:
        decoded_token = decode_refresh_token(refresh_token, SECRET_KEY)
        if decoded_token:
            user_id = decoded_token.get('user_id')
            # Fetch user data from the database using the user_id
            user = users_collection.find_one({'_id': ObjectId(user_id)})
            if user:
                name = user['name']
                return jsonify({'name': name}), 200
            else:
                return jsonify({'message': 'User not found'}), 404
    except jwt.ExpiredSignatureError:
        return False
    except jwt.InvalidTokenError:
        return False


def check_token(access_token):
    if not access_token:
        response = jsonify({'token': False}), 401
        return False
    try:
        # Verify the JWT token
        decoded_token = jwt.decode(access_token, SECRET_KEY, algorithms=['HS256'])
        return True
    except jwt.ExpiredSignatureError:
        response = jsonify({'token': False}), 401
        return False
    except jwt.InvalidTokenError:
        response = jsonify({'token': False}), 401
        return False


@application.route('/validate_tokens', methods=['POST'])
def validate_tokens():
    data = request.get_json()
    access_token = data.get('access_token')
    refresh_token = data.get('refresh_token')

    if not access_token and not refresh_token:
        response = jsonify({'message': 'Access token or refresh token is missing'}), 401
        return response

    if access_token:
        return verify_access_token(access_token)
    if refresh_token:
        return verify_refresh_token(refresh_token)


@application.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    email = data.get('email')
    password = data.get('password')

    # Check if the user exists in the database
    user = users_collection.find_one({'email': email})

    if user:
        hashed_password_in_db = user.get('password', '')

        if bcrypt.check_password_hash(hashed_password_in_db, password):
            user_id = str(user['_id'])  # Assuming user ID is stored as ObjectId in MongoDB

            # Generate tokens based on user ID
            access_token = jwt.encode(
                {'user_id': user_id, 'exp': datetime.utcnow() + timedelta(minutes=30)},
                application.config['SECRET_KEY'], algorithm='HS256')
            refresh_token = jwt.encode(
                {'user_id': user_id, 'exp': datetime.utcnow() + timedelta(days=1)},
                application.config['SECRET_KEY'], algorithm='HS256')

            response = jsonify({'access_token': access_token, 'refresh_token': refresh_token}), 200
            return response

    response = jsonify({'message': 'Invalid credentials'}), 401
    return response


@application.route('/register', methods=['POST'])
def register():
    data = request.get_json()
    name = data.get('name')
    email = data.get('email')
    password = data.get('password')

    hashed_password = bcrypt.generate_password_hash(password).decode('utf-8')

    document = {
        'name': name,
        'email': email,
        'password': hashed_password
    }

    is_present = users_collection.find_one({'email': email})

    if (is_present is None) and (bcrypt.check_password_hash(hashed_password, password)):
        users_collection.insert_one(document)
        response = jsonify({'message': True}), 200
        return response
    else:
        response = jsonify({'message': False}), 401
        return response


@application.route('/add_task', methods=['POST'])
def add_task():
    data = request.get_json()
    access_token = data.get('access_token')

    if check_token(access_token) is False:
        return jsonify({'token': False}), 401

    headline = data.get('headline')
    responsible = data.get('responsible')
    deadline = data.get('deadline')
    description = data.get('description')
    status = data.get('status', None)

    if status:
        status_doc = statuses_collection.find_one({'status': status})
        if status_doc:
            del status_doc['_id']

    today = datetime.today()
    formatted_date = today.strftime("%a %b %d %Y")

    document = {
        'date': formatted_date,
        'headline': headline,
        'responsible': responsible,
        'deadline': deadline,
        'description': description,
        'status': status_doc
    }

    tasks_collection.insert_one(document)

    # Find the user's email
    user = users_collection.find_one({'name': responsible})
    if user:
        user_email = user.get('email')
        if user_email:
            # Prepare the email
            subject = "New Task Assigned"
            body = f"""
            Hello {responsible},

            A new task has been assigned to you.

            Task Details:
            Headline: {headline}
            Description: {description}
            Deadline: {deadline}

            Best Regards,
            Your Task Management System
            """
            msg = Message(subject=subject,
                          recipients=[user_email],
                          body=body)
            try:
                mail.send(msg)
            except Exception as e:
                return jsonify({'message': 'Email failed to send', 'error': str(e)}), 500

    return jsonify({'message': True}), 200


@application.route('/update_task', methods=['POST'])
def update_task():
    data = request.get_json()
    access_token = data.get('access_token')
    if check_token(access_token) is False:
        return jsonify({'token': False}), 401

    task_id = data.get('task_id')
    task = tasks_collection.find_one({'_id': ObjectId(task_id)})
    if task is None:
        return jsonify({'message': False}), 404

    # Update fields based on the provided data
    task['headline'] = data.get('headline', task['headline'])
    task['responsible'] = data.get('responsible', task['responsible'])
    task['deadline'] = data.get('deadline', task['deadline'])
    task['description'] = data.get('description', task['description'])
    status = data.get('status')
    if status:
        status_doc = statuses_collection.find_one({'status': status})
        if status_doc:
            del status_doc['_id']
        task['status'] = status_doc

    # Update the task in the database
    tasks_collection.update_one({'_id': ObjectId(task_id)}, {'$set': task})
    return jsonify({'message': True}), 200


@application.route('/delete_task', methods=['POST'])
def delete_task():
    data = request.get_json()
    access_token = data.get('access_token')
    if check_token(access_token) is False:
        return jsonify({'token': False}), 401

    task_ids = data.get('task_ids')
    for task_id in task_ids:
        tasks_collection.find_one_and_delete({'_id': ObjectId(task_id)})
    return jsonify({'message': True}), 200


@application.route('/task_info', methods=['POST'])
def task_info():
    data = request.get_json()
    access_token = data.get('access_token')
    if check_token(access_token) is False:
        return jsonify({'token': False}), 401

    task_id = data.get('task_id')
    object_id = ObjectId(task_id)
    task_document = tasks_collection.find_one({'_id': object_id})

    if task_document:
        task_document['_id'] = str(task_document['_id'])

        # Use dumps() to handle ObjectId serialization
        return json.dumps(task_document, default=str), 200, {'Content-Type': 'application/json'}
    else:
        response = jsonify({'message': 'Task not found'}), 404
        return response


@application.route('/tasks', methods=['POST'])
def tasks():
    data = request.get_json()
    access_token = data.get('access_token')
    if check_token(access_token) is False:
        return jsonify({'token': False}), 401
    keyword = data.get('keyword')
    page = data.get('page', 1)
    per_page = data.get('per_page', 10)

    filter_criteria = {}
    if keyword:
        tasks_collection.create_index([("$**", "text")])
        filter_criteria['$text'] = {'$search': keyword}

    total_tasks = tasks_collection.count_documents(filter_criteria)

    total_pages = math.ceil(total_tasks / per_page)

    # Paginate the query results using skip and limit, and apply filters
    skip = (page - 1) * per_page
    documents = list(tasks_collection.find(filter_criteria).skip(skip).limit(per_page))
    for document in documents:
        document['_id'] = str(document['_id'])

    # Calculate the range being displayed
    start_range = skip + 1
    end_range = min(skip + per_page, total_tasks)

    # Serialize the documents using json_util from pymongo and specify encoding
    response = Response(json_util.dumps(
        {'tasks': documents, 'total_tasks': total_tasks, 'start_range': start_range, 'end_range': end_range,
         'total_pages': total_pages},
        ensure_ascii=False).encode('utf-8'),
                        content_type='application/json;charset=utf-8')
    return response


@application.route('/new_status', methods=['POST'])
def new_status():
    data = request.get_json()
    access_token = data.get('access_token')
    if check_token(access_token) is False:
        return jsonify({'token': False}), 401

    status = data.get('status')
    colour = data.get('colour')
    type = data.get('type')
    is_present = statuses_collection.find_one({'status': status, 'type': type})
    if is_present is None:
        statuses_collection.insert_one({'status': status, 'colour': colour, 'type': type})
        return jsonify({'message': 'Created successfully'}), 200
    else:
        return jsonify({'message': 'Status already exists'}), 409


@application.route('/get_statuses', methods=['POST'])
def get_statuses():
    data = request.get_json()
    access_token = data.get('access_token')
    if check_token(access_token) is False:
        return jsonify({'token': False}), 401
    type = data.get('type')
    filter_criteria = {}
    if type:
        statuses_collection.create_index([("$**", "text")])
        filter_criteria['$text'] = {'$search': type}

    documents = list(statuses_collection.find(filter_criteria))
    for document in documents:
        document['_id'] = str(document['_id'])

    response = Response(json_util.dumps(
        {'statuses': documents},
        ensure_ascii=False).encode('utf-8'),
                        content_type='application/json;charset=utf-8')
    return response, 200


@application.route('/users', methods=['POST'])
def users():
    data = request.get_json()
    access_token = data.get('access_token')
    if check_token(access_token) is False:
        return jsonify({'token': False}), 401
    keyword = data.get('keyword')
    page = data.get('page', 1)
    per_page = data.get('per_page', 10)

    filter_criteria = {}
    if keyword:
        users_collection.create_index([("$**", "text")])
        filter_criteria['$text'] = {'$search': keyword}

    total_users = users_collection.count_documents(filter_criteria)

    total_pages = math.ceil(total_users / per_page)

    skip = (page - 1) * per_page
    documents = list(users_collection.find(filter_criteria).skip(skip).limit(per_page))
    for document in documents:
        document['_id'] = str(document['_id'])

    start_range = skip + 1
    end_range = min(skip + per_page, total_users)

    # Serialize the documents using json_util from pymongo and specify encoding
    response = Response(json_util.dumps(
        {'users': documents, 'total_users': total_users, 'start_range': start_range, 'end_range': end_range,
         'total_pages': total_pages},
        ensure_ascii=False).encode('utf-8'),
                        content_type='application/json;charset=utf-8')
    return response, 200


def upload_contract_to_s3(contract, unique_filename):
    # Create an in-memory file-like object
    file_stream = io.BytesIO()
    contract.save(file_stream)
    file_stream.seek(0)

    # Upload the file directly to S3
    config.s3_client.upload_fileobj(
        file_stream,
        'olimpiabucket',
        f'contracts_clients/{unique_filename}',
        ExtraArgs={'ACL': 'public-read'}
    )

def generate_unique_filename(original_filename):
    current_timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')[:-3]

    unique_identifier = str(uuid.uuid4())

    # Extract the file extension from the original filename
    file_extension = original_filename.rsplit('.', 1)[-1].lower()

    unique_filename = f"{current_timestamp}_{unique_identifier}.{file_extension}"

    return unique_filename


@application.route('/add_contract', methods=['POST'])
def add_contract():
    data = request.form
    access_token = data.get('access_token')

    # Check token
    if check_token(access_token) is False:
        return jsonify({'token': False}), 401

    # Extract contract data
    number = data.get('number')
    counterpartie = data.get('counterpartie')
    category = data.get('category')
    date = data.get('date')
    deadline = data.get('deadline')
    subject = data.get('subject')
    status = data.get('status', None)
    original_document = data.get('original_document', None)
    is_valid = data.get('is_valid', None)
    subwarehouse = data.get('subwarehouse')  # Extract subwarehouse field

    # Handle status
    status_doc = None
    if status:
        status_doc = statuses_collection.find_one({'status': status})
        if status_doc:
            del status_doc['_id']

    # Handle scans upload
    scans = request.files.getlist('scans')
    scans_links_list = []
    for scan in scans:
        # Create in-memory file object
        file_stream = io.BytesIO()
        scan.save(file_stream)
        file_stream.seek(0)

        unique_filename = generate_unique_filename(scan.filename)

        # Upload file to S3
        config.s3_client.upload_fileobj(file_stream, 'olimpiabucket', f'contracts/{unique_filename}',
                                        ExtraArgs={'ACL': 'public-read'})
        scans_links_list.append(f'https://olimpiabucket.fra1.digitaloceanspaces.com/contracts/{unique_filename}')

    # Create the document
    document = {
        'date': date,
        'number': number,
        'counterpartie': counterpartie,
        'category': category,
        'deadline': deadline,
        'subject': subject,
        'status': status_doc,
        'original_document': original_document,
        'is_valid': is_valid,
        'subwarehouse': subwarehouse,  # Add subwarehouse to the document
        'scans_links': scans_links_list
    }

    # Insert document into the database
    contracts_collection.insert_one(document)
    return jsonify({'message': True}), 200


@application.route('/update_contract', methods=['POST'])
def update_contract():
    data = request.form
    access_token = data.get('access_token')

    # Check token
    if check_token(access_token) is False:
        return jsonify({'token': False}), 401

    # Find contract by ID
    contract_id = data.get('contract_id')
    contract = contracts_collection.find_one({'_id': ObjectId(contract_id)})

    if contract is None:
        return jsonify({'message': False}), 404

    # Update fields
    contract['date'] = data.get('date', contract['date'])
    contract['number'] = data.get('number', contract['number'])
    contract['counterpartie'] = data.get('counterpartie', contract['counterpartie'])
    contract['category'] = data.get('category', contract['category'])
    contract['deadline'] = data.get('deadline', contract['deadline'])
    contract['subject'] = data.get('subject', contract['subject'])
    contract['original_document'] = data.get('original_document', contract['original_document'])
    contract['is_valid'] = data.get('is_valid', contract['is_valid'])
    contract['subwarehouse'] = data.get('subwarehouse', contract.get('subwarehouse'))  # Update subwarehouse field

    # Handle status
    status = data.get('status')
    if status:
        status_doc = statuses_collection.find_one({'status': status})
        if status_doc:
            del status_doc['_id']
        contract['status'] = status_doc

    # Update contract in the database
    contracts_collection.update_one({'_id': ObjectId(contract_id)}, {'$set': contract})

    # Handle scans deletion
    delete_scans = data.get('delete_scans')
    if delete_scans:
        for scan_link in contract['scans_links']:
            if scan_link in delete_scans:
                file_key = scan_link.split('/')[-1]
                config.s3_client.delete_object(Bucket='olimpiabucket', Key=f'contracts/{file_key}')
                contracts_collection.find_one_and_update({'_id': ObjectId(contract_id)},
                                                         {"$pull": {
                                                             "scans_links": f"https://olimpiabucket.fra1.digitaloceanspaces.com/contracts/{file_key}"}})

    # Handle scans upload
    scans = request.files.getlist('scans')
    if scans:
        for scan in scans:
            # Create in-memory file object
            file_stream = io.BytesIO()
            scan.save(file_stream)
            file_stream.seek(0)

            unique_filename = generate_unique_filename(scan.filename)

            # Upload file to S3
            config.s3_client.upload_fileobj(file_stream, 'olimpiabucket', f'contracts/{unique_filename}',
                                            ExtraArgs={'ACL': 'public-read'})
            contracts_collection.find_one_and_update({'_id': ObjectId(contract_id)},
                                                     {"$push": {
                                                         "scans_links": f"https://olimpiabucket.fra1.digitaloceanspaces.com/contracts/{unique_filename}"}})

    return jsonify({'message': True}), 200


@application.route('/delete_contract', methods=['POST'])
def delete_contract():
    data = request.get_json()
    access_token = data.get('access_token')
    if check_token(access_token) is False:
        return jsonify({'token': False}), 401

    contract_ids = data.get('contract_ids')
    for contract_id in contract_ids:
        contracts_collection.find_one_and_delete({'_id': ObjectId(contract_id)})
    return jsonify({'message': True}), 200


@application.route('/contract_info', methods=['POST'])
def contract_info():
    data = request.get_json()
    access_token = data.get('access_token')
    if check_token(access_token) is False:
        return jsonify({'token': False}), 401

    contract_id = data.get('contract_id')
    object_id = ObjectId(contract_id)
    contract_document = contracts_collection.find_one({'_id': object_id})

    if contract_document:
        contract_document['_id'] = str(contract_document['_id'])

        # Use dumps() to handle ObjectId serialization
        return json.dumps(contract_document, default=str), 200, {'Content-Type': 'application/json'}
    else:
        response = jsonify({'message': 'Contract not found'}), 404
        return response


@application.route('/contracts', methods=['POST'])
def contracts():
    data = request.get_json()
    access_token = data.get('access_token')
    if check_token(access_token) is False:
        return jsonify({'token': False}), 401
    keyword = data.get('keyword')
    page = data.get('page', 1)
    per_page = data.get('per_page', 10)

    filter_criteria = {}
    if keyword:
        contracts_collection.create_index([("$**", "text")])
        filter_criteria['$text'] = {'$search': keyword}

    total_contracts = contracts_collection.count_documents(filter_criteria)

    total_pages = math.ceil(total_contracts / per_page)

    # Paginate the query results using skip and limit, and apply filters
    skip = (page - 1) * per_page
    documents = list(contracts_collection.find(filter_criteria).skip(skip).limit(per_page))
    for document in documents:
        document['_id'] = str(document['_id'])

    # Calculate the range being displayed
    start_range = skip + 1
    end_range = min(skip + per_page, total_contracts)

    # Serialize the documents using json_util from pymongo and specify encoding
    response = Response(json_util.dumps(
        {'contracts': documents, 'total_contracts': total_contracts, 'start_range': start_range, 'end_range': end_range,
         'total_pages': total_pages},
        ensure_ascii=False).encode('utf-8'),
                        content_type='application/json;charset=utf-8')
    return response


@application.route('/merchants_reports', methods=['POST'])
def merchants_reports():
    data = request.get_json()
    access_token = data.get('access_token')
    if check_token(access_token) is False:
        return jsonify({'token': False}), 401
    keyword = data.get('keyword')
    page = data.get('page', 1)
    per_page = data.get('per_page', 10)

    filter_criteria = {}
    if keyword:
        merchants_reports_collection.create_index([("$**", "text")])
        filter_criteria['$text'] = {'$search': keyword}

    total_reports = merchants_reports_collection.count_documents(filter_criteria)

    total_pages = math.ceil(total_reports / per_page)

    # Paginate the query results using skip and limit, and apply filters
    skip = (page - 1) * per_page
    documents = list(merchants_reports_collection.find(filter_criteria).skip(skip).limit(per_page))
    for document in documents:
        document['_id'] = str(document['_id'])

    # Calculate the range being displayed
    start_range = skip + 1
    end_range = min(skip + per_page, total_reports)

    # Serialize the documents using json_util from pymongo and specify encoding
    response = Response(json_util.dumps(
        {'reports': documents, 'total_reports': total_reports, 'start_range': start_range, 'end_range': end_range,
         'total_pages': total_pages},
        ensure_ascii=False).encode('utf-8'),
                        content_type='application/json;charset=utf-8')
    return response


@application.route('/merchants_reports_update', methods=['POST'])
def merchants_reports_update():
    data = request.get_json()
    access_token = data.get('access_token')
    if check_token(access_token) is False:
        return jsonify({'token': False}), 401

    report_id = data.get('report_id')
    report = merchants_reports_collection.find_one({'_id': ObjectId(report_id)})
    if report is None:
        return jsonify({'message': False}), 404

    # Update fields based on the provided data
    report['shop_name'] = data.get('shop_name', report['shop_name'])
    report['product_name'] = data.get('product_name', report['product_name'])
    report['product_amount'] = data.get('product_amount', report['product_amount'])
    report['sale_amount'] = data.get('sale_amount', report['sale_amount'])
    report['photo'] = data.get('photo', report['photo'])

    # Update the report in the database
    merchants_reports_collection.update_one({'_id': ObjectId(report_id)}, {'$set': report})
    return jsonify({'message': True}), 200


@application.route('/merchants_reports_delete', methods=['POST'])
def merchants_reports_delete():
    data = request.get_json()
    access_token = data.get('access_token')
    if check_token(access_token) is False:
        return jsonify({'token': False}), 401

    reports_ids = data.get('reports_ids')
    for report_id in reports_ids:
        merchants_reports_collection.find_one_and_delete({'_id': ObjectId(report_id)})
    return jsonify({'message': True}), 200


@application.route('/merchant_report_info', methods=['POST'])
def merchant_report_info():
    data = request.get_json()
    access_token = data.get('access_token')
    if check_token(access_token) is False:
        return jsonify({'token': False}), 401

    report_id = data.get('report_id')
    object_id = ObjectId(report_id)
    report_document = merchants_reports_collection.find_one({'_id': object_id})

    if report_document:
        report_document['_id'] = str(report_document['_id'])

        # Use dumps() to handle ObjectId serialization
        return json.dumps(report_document, default=str), 200, {'Content-Type': 'application/json'}
    else:
        response = jsonify({'message': 'Report not found'}), 404
        return response


@application.route('/add_client', methods=['POST'])
def add_client():
    data = request.form
    access_token = data.get('access_token')
    if check_token(access_token) is False:
        return jsonify({'token': False}), 401
    name = data.get('name')
    edrpou = data.get('edrpou')
    ipn = data.get('ipn')
    bank = data.get('bank')
    account_number = data.get('account_number')
    address_jur = data.get('address_jur')
    address_phiz = data.get('address_phiz')
    address_sklad = data.get('address_sklad')
    pib_kerivnyka = data.get('pib_kerivnyka')
    pib_kontaktna = data.get('pib_kontaktna')
    number = data.get('number')
    email = data.get('email')
    supervisors = data.get('supervisors')
    if supervisors:
        supervisors = json.loads(supervisors)
    contracts = request.files.getlist('contracts')
    payment_terms_and_conditions = data.get('payment_terms_and_conditions')

    document = {'name': name,
                'edrpou': edrpou,
                'ipn': ipn,
                'bank': bank,
                'account_number': account_number,
                'address_jur': address_jur,
                'address_phiz': address_phiz,
                'address_sklad': address_sklad,
                'pib_kerivnyka': pib_kerivnyka,
                'pib_kontaktna': pib_kontaktna,
                'number': number,
                'email': email,
                'supervisors': supervisors,
                'payment_terms_and_conditions': payment_terms_and_conditions}

    contracts_links_list = []
    for contract in contracts:
        # Create an in-memory file-like object
        file_stream = io.BytesIO()
        contract.save(file_stream)
        file_stream.seek(0)

        def generate_unique_filename(original_filename):
            current_timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')[:-3]

            unique_identifier = str(uuid.uuid4())

            # Extract the file extension from the original filename
            file_extension = original_filename.rsplit('.', 1)[-1].lower()

            unique_filename = f"{current_timestamp}_{unique_identifier}.{file_extension}"

            return unique_filename

        unique_filename = generate_unique_filename(contract.filename)

        # Upload the file directly to S3
        config.s3_client.upload_fileobj(file_stream, 'olimpiabucket', f'contracts_clients/{unique_filename}',
                                        ExtraArgs={'ACL': 'public-read'})
        contracts_links_list.append(f'https://olimpiabucket.fra1.digitaloceanspaces.com/contracts_clients/{unique_filename}')

    document['contracts_links'] = contracts_links_list

    clients_collection.insert_one(document)
    return jsonify({'message': True}), 200


@application.route('/update_client', methods=['POST'])
def update_client():
    data = request.form
    access_token = data.get('access_token')
    if check_token(access_token) is False:
        return jsonify({'token': False}), 401

    client_id = data.get('client_id')
    client = clients_collection.find_one({'_id': ObjectId(client_id)})
    if client is None:
        return jsonify({'message': False}), 404

    # Update fields based on the provided data
    client['name'] = data.get('name', client['name'])
    client['edrpou'] = data.get('edrpou', client['edrpou'])
    client['ipn'] = data.get('ipn', client['ipn'])
    client['bank'] = data.get('bank', client['bank'])
    client['account_number'] = data.get('account_number', client['account_number'])
    client['address_jur'] = data.get('address_jur', client['address_jur'])
    client['address_phiz'] = data.get('address_phiz', client['address_phiz'])
    client['address_sklad'] = data.get('address_sklad', client['address_sklad'])
    client['pib_kerivnyka'] = data.get('pib_kerivnyka', client['pib_kerivnyka'])
    client['pib_kontaktna'] = data.get('pib_kontaktna', client['pib_kontaktna'])
    client['number'] = data.get('number', client['number'])
    client['email'] = data.get('email', client['email'])
    client['supervisors'] = json.loads(data.get('supervisors', client['supervisors']))
    client['payment_terms_and_conditions'] = data.get('payment_terms_and_conditions', client['payment_terms_and_conditions'])
    clients_collection.update_one({'_id': ObjectId(client_id)}, {'$set': client})

    delete_contracts = data.get('delete_contracts')
    contracts = request.files.getlist('contracts')

    if delete_contracts:
        # Delete files not in new scans_links but present in old contract['scans_links']
        for contract_link in client['contracts_links']:
            if contract_link in delete_contracts:
                # Extract file key from the old_scan_link
                file_key = contract_link.split('/')[-1]
                config.s3_client.delete_object(Bucket='olimpiabucket', Key=f'contracts_clients/{file_key}')
                clients_collection.find_one_and_update({'_id': ObjectId(client_id)},
                                                         {"$pull": {"contracts_links": f"https://olimpiabucket.fra1.digitaloceanspaces.com/contracts/{file_key}"}})

    if contracts:
        for contract in contracts:
            # Create an in-memory file-like object
            file_stream = io.BytesIO()
            contract.save(file_stream)
            file_stream.seek(0)

            def generate_unique_filename(original_filename):
                current_timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')[:-3]

                unique_identifier = str(uuid.uuid4())

                # Extract the file extension from the original filename
                file_extension = original_filename.rsplit('.', 1)[-1].lower()

                unique_filename = f"{current_timestamp}_{unique_identifier}.{file_extension}"

                return unique_filename

            unique_filename = generate_unique_filename(contract.filename)

            # Upload the file directly to S3
            config.s3_client.upload_fileobj(file_stream, 'olimpiabucket', f'contracts_clients/{unique_filename}', ExtraArgs={'ACL': 'public-read'})
            clients_collection.find_one_and_update({'_id': ObjectId(client_id)},
                                                     {"$push": {"contracts_links": f"https://olimpiabucket.fra1.digitaloceanspaces.com/contracts/{unique_filename}"}})

    return jsonify({'message': True}), 200


@application.route('/delete_client', methods=['POST'])
def delete_client():
    data = request.get_json()
    access_token = data.get('access_token')
    if check_token(access_token) is False:
        return jsonify({'token': False}), 401

    clients_ids = data.get('clients_ids')
    for client_id in clients_ids:
        clients_collection.find_one_and_delete({'_id': ObjectId(client_id)})
    return jsonify({'message': True}), 200


@application.route('/client_info', methods=['POST'])
def client_info():
    data = request.get_json()
    access_token = data.get('access_token')
    if check_token(access_token) is False:
        return jsonify({'token': False}), 401

    client_id = data.get('client_id')
    object_id = ObjectId(client_id)
    client_document = clients_collection.find_one({'_id': object_id})

    if client_document:
        client_document['_id'] = str(client_document['_id'])

        # Use dumps() to handle ObjectId serialization
        return json.dumps(client_document, default=str), 200, {'Content-Type': 'application/json'}
    else:
        response = jsonify({'message': 'Client not found'}), 404
        return response


@application.route('/clients', methods=['POST'])
def clients():
    data = request.get_json()
    access_token = data.get('access_token')
    if check_token(access_token) is False:
        return jsonify({'token': False}), 401
    keyword = data.get('keyword')
    page = data.get('page', 1)
    per_page = data.get('per_page', 10)

    filter_criteria = {}
    if keyword:
        clients_collection.create_index([("$**", "text")])
        filter_criteria['$text'] = {'$search': keyword}

    total_clients = clients_collection.count_documents(filter_criteria)

    total_pages = math.ceil(total_clients / per_page)

    # Paginate the query results using skip and limit, and apply filters
    skip = (page - 1) * per_page
    documents = list(clients_collection.find(filter_criteria).skip(skip).limit(per_page))
    for document in documents:
        document['_id'] = str(document['_id'])

    # Calculate the range being displayed
    start_range = skip + 1
    end_range = min(skip + per_page, total_clients)

    # Serialize the documents using json_util from pymongo and specify encoding
    response = Response(json_util.dumps(
        {'clients': documents, 'total_clients': total_clients, 'start_range': start_range, 'end_range': end_range,
         'total_pages': total_pages},
        ensure_ascii=False).encode('utf-8'),
                        content_type='application/json;charset=utf-8')
    return response


@application.route('/update_order', methods=['POST'])
def update_order():
    data = request.get_json()
    access_token = data.get('access_token')
    if check_token(access_token) is False:
        return jsonify({'token': False}), 401

    order_id = data.get('order_id')
    order = orders_collection.find_one({'_id': ObjectId(order_id)})
    if order is None:
        return jsonify({'message': False}), 404

    # Update fields based on the provided data
    order['sales_agent'] = data.get('sales_agent', order.get('sales_agent'))
    order['distributor'] = data.get('distributor', order.get('distributor'))
    order['shop'] = data.get('shop', order.get('shop'))
    order['product'] = data.get('product', order.get('product'))
    order['comment'] = data.get('comment', order.get('comment'))
    order['order_number'] = data.get('order_number', order.get('order_number'))
    order['date'] = data.get('date', order.get('date'))
    order['counterpartie_code'] = data.get('counterpartie_code', order.get('counterpartie_code'))
    order['order_number_1c'] = data.get('order_number_1c', order.get('order_number_1c'))

    status = data.get('status')
    if status:
        status_doc = statuses_collection.find_one({'status': status})
        if status_doc:
            del status_doc['_id']
        order['status'] = status_doc

    order['photos'] = data.get('photos', order.get('photos'))

    # Update the order in the database
    orders_collection.update_one({'_id': ObjectId(order_id)}, {'$set': order})

    # Recalculate totals
    order = orders_collection.find_one({'_id': ObjectId(order_id)})
    total_amount = 0
    total_amount_discount = 0
    for product in order['product']:
        if product.get('discount', '0') == '0':
            total_amount += int(product.get('amount', 0))
        else:
            total_amount_discount += int(product.get('amount', 0))
    order['total_amount'] = total_amount
    order['total_amount_discount'] = total_amount_discount

    # Update totals in the database
    orders_collection.update_one({'_id': ObjectId(order_id)}, {'$set': order})

    return jsonify({'message': True}), 200


@application.route('/delete_order', methods=['POST'])
def delete_order():
    data = request.get_json()
    access_token = data.get('access_token')
    if check_token(access_token) is False:
        return jsonify({'token': False}), 401

    orders_ids = data.get('orders_ids')
    for order_id in orders_ids:
        orders_collection.find_one_and_delete({'_id': ObjectId(order_id)})
    return jsonify({'message': True}), 200


@application.route('/order_info', methods=['POST'])
def order_info():
    data = request.get_json()
    access_token = data.get('access_token')
    if check_token(access_token) is False:
        return jsonify({'token': False}), 401

    order_id = data.get('order_id')
    object_id = ObjectId(order_id)
    order_document = orders_collection.find_one({'_id': object_id})

    if order_document:
        order_number_list = []
        order_number_list.append(order_document['order_number_1c'])
        request_payment = requests.post('https://olimpia.comp.lviv.ua:8189/BaseWeb/hs/base?action=getpaymentstatus',
                                        data={"order": order_number_list}, auth=('CRM', 'CegJr6YcK1sTnljgTIly'))
        root = ET.fromstring(request_payment.text)
        payment_answer = root.text
        payment_status = payment_answer

        order_document['_id'] = str(order_document['_id'])
        order_document['payment_status'] = payment_status

        # Use dumps() to handle ObjectId serialization
        return json.dumps(order_document, default=str), 200, {'Content-Type': 'application/json'}
    else:
        response = jsonify({'message': 'Order not found'}), 404
        return response


@application.route('/orders', methods=['POST'])
def orders():
    data = request.get_json()
    access_token = data.get('access_token')
    if check_token(access_token) is False:
        return jsonify({'token': False}), 401
    keyword = data.get('keyword')
    page = data.get('page', 1)
    per_page = data.get('per_page', 10)

    filter_criteria = {}
    if keyword:
        orders_collection.create_index([("$**", "text")])
        filter_criteria['$text'] = {'$search': keyword}

    total_orders = orders_collection.count_documents(filter_criteria)

    total_pages = math.ceil(total_orders / per_page)

    # Paginate the query results using skip and limit, and apply filters
    skip = (page - 1) * per_page
    documents = list(orders_collection.find(filter_criteria).skip(skip).limit(per_page))
    for document in documents:
        document['_id'] = str(document['_id'])

    # Calculate the range being displayed
    start_range = skip + 1
    end_range = min(skip + per_page, total_orders)

    # Serialize the documents using json_util from pymongo and specify encoding
    response = Response(json_util.dumps(
        {'orders': documents, 'total_orders': total_orders, 'start_range': start_range, 'end_range': end_range,
         'total_pages': total_pages},
        ensure_ascii=False).encode('utf-8'),
                        content_type='application/json;charset=utf-8')
    return response


@application.route('/products', methods=['POST'])
def products():
    data = request.get_json()
    access_token = data.get('access_token')
    if not check_token(access_token):
        return jsonify({'token': False}), 401

    username = 'CRM'
    password = 'CegJr6YcK1sTnljgTIly'
    urls = ['https://olimpia.comp.lviv.ua:8189/BaseWeb/hs/base?action=getreportrest',
            'https://olimpia.comp.lviv.ua:8189/BaseWeb1/hs/base?action=getreportrest']

    # Fetch existing products once and store in a dictionary
    existing_products = products_collection.find({}, {'code': 1, 'recommended_rest': 1})
    existing_products_dict = {prod['code']: prod.get('recommended_rest', '') for prod in existing_products if 'code' in prod}

    bulk_operations = []

    for url in urls:
        # Retrieve data from external API
        response = requests.get(url, auth=(username, password))
        xml_string = response.text

        # Parse XML response
        root = ET.fromstring(xml_string)

        # Prepare bulk operations
        for product in root.findall('Product'):
            code = product.get('Code')
            good = product.get('Good')
            rest = product.get('Rest')
            series = product.get('Series')
            type = product.get('Type')
            sort = product.get('Sort')

            if type == "1":
                warehouse = 'Склад Сировини'
            elif type == '2':
                warehouse = 'Склад Готової продукції'

            if url == 'https://olimpia.comp.lviv.ua:8189/BaseWeb1/hs/base?action=getreportrest' and sort == '1':
                subwarehouse = 'Фастпол'
                sort = 'Бобо'
            elif url == 'https://olimpia.comp.lviv.ua:8189/BaseWeb1/hs/base?action=getreportrest' and sort == '2':
                subwarehouse = 'Фастпол'
                sort = 'Печиво'
            else:
                subwarehouse = 'Етрус'

            # Retrieve the current 'recommended_rest' from the dictionary
            recommended_rest = existing_products_dict.get(code, '')

            document = {
                'code': code,
                'good': good,
                'rest': rest,
                'series': series,
                'warehouse': warehouse,
                'subwarehouse': subwarehouse,
                'sort': sort,
                'recommended_rest': recommended_rest,
                'type': '1c'
            }

            bulk_operations.append(pymongo.UpdateOne(
                {'code': code},
                {'$set': document},
                upsert=True
            ))

    # Execute bulk operations in batches to optimize performance
    if bulk_operations:
        batch_size = 1000
        for i in range(0, len(bulk_operations), batch_size):
            products_collection.bulk_write(bulk_operations[i:i + batch_size])

    # Pagination and filtering
    keyword = data.get('keyword')
    page = data.get('page', 1)
    per_page = data.get('per_page', 10)
    warehouse = data.get('warehouse')
    subwarehouse = data.get('subwarehouse')
    sort = data.get('sort')

    filter_criteria = {}
    if keyword:
        regex_pattern = f'.*{re.escape(keyword)}.*'
        filter_criteria['good'] = {'$regex': regex_pattern, '$options': 'i'}
    if warehouse:
        regex_pattern = f'.*{re.escape(warehouse)}.*'
        filter_criteria['warehouse'] = {'$regex': regex_pattern, '$options': 'i'}
    if subwarehouse:
        regex_pattern = f'.*{re.escape(subwarehouse)}.*'
        filter_criteria['subwarehouse'] = {'$regex': regex_pattern, '$options': 'i'}
    if sort:
        regex_pattern = f'.*{re.escape(sort)}.*'
        filter_criteria['sort'] = {'$regex': regex_pattern, '$options': 'i'}

    total_products = products_collection.count_documents(filter_criteria)
    total_pages = math.ceil(total_products / per_page)
    skip = (page - 1) * per_page
    documents = list(products_collection.find(filter_criteria).skip(skip).limit(per_page))

    for document in documents:
        document['_id'] = str(document['_id'])

    start_range = skip + 1
    end_range = min(skip + per_page, total_products)

    response = Response(
        json_util.dumps({
            'products': documents,
            'total_products': total_products,
            'start_range': start_range,
            'end_range': end_range,
            'total_pages': total_pages
        }, ensure_ascii=False).encode('utf-8'),
        content_type='application/json;charset=utf-8'
    )

    return response


@application.route('/add_product', methods=['POST'])
def add_product():
    data = request.form
    access_token = data.get('access_token')

    if not check_token(access_token):
        return jsonify({'token': False}), 401

    product_type = data.get('type')
    subwarehouse = data.get('subwarehouse')  # Extract subwarehouse from form data

    # Handle workwear product type
    if product_type == 'workwear':
        employee = data.get('employee')
        name = data.get('name')
        date = data.get('date')
        price = data.get('price')
        lifetime = data.get('lifetime')
        rest = data.get('rest')
        recommended_rest = data.get('recommended_rest', None)

        document = {
            'employee': employee,
            'name': name,
            'date': date,
            'price': price,
            'lifetime': lifetime,
            'rest': rest,
            'warehouse': 'Склад Спецодягу',
            'subwarehouse': subwarehouse,  # Use the subwarehouse field
            'recommended_rest': recommended_rest,
            'type': 'workwear'
        }

    # Handle distributor product type
    elif product_type == 'distributor':
        distributor = data.get('distributor')
        name = data.get('name')
        amount = data.get('amount')
        price = data.get('price')
        sum = float(amount) * float(price)
        recommended_rest = data.get('recommended_rest', None)

        document = {
            'name': name,
            'amount': amount,
            'price': price,
            'sum': sum,
            'warehouse': "Склад Дистриб'ютора",
            'subwarehouse': subwarehouse,  # Use the subwarehouse field
            'recommended_rest': recommended_rest,
            'type': 'distributor'
        }

        # Handle contracts
        contracts = request.files.getlist('contracts')
        contracts_links_list = []

        for contract in contracts:
            unique_filename = generate_unique_filename(contract.filename)

            upload_contract_to_s3(contract, unique_filename)

            contracts_links_list.append(
                f'https://olimpiabucket.fra1.digitaloceanspaces.com/contracts_clients/{unique_filename}')

        document['contracts_links'] = contracts_links_list

    else:
        return jsonify({'message': False}), 400

    products_collection.insert_one(document)

    return jsonify({'message': True}), 200


@application.route('/product_info', methods=['POST'])
def product_info():
    data = request.get_json()
    access_token = data.get('access_token')
    if check_token(access_token) is False:
        return jsonify({'token': False}), 401

    product_code = data.get('product_code', None)
    product_id = data.get('product_id', None)
    if product_code:
        product_document = products_collection.find_one({'code': product_code})
    elif product_id:
        product_document = products_collection.find_one({'_id': ObjectId(product_id)})

    if product_document:
        product_document['_id'] = str(product_document['_id'])

        # Use dumps() to handle ObjectId serialization
        return json.dumps(product_document, default=str), 200, {'Content-Type': 'application/json'}
    else:
        response = jsonify({'message': 'Product not found'}), 404
        return response


@application.route('/update_product', methods=['POST'])
def update_product():
    data = request.form
    product_code = data.get('product_code')
    access_token = data.get('access_token')

    if not check_token(access_token):
        return jsonify({'token': False}), 401

    # Find the existing product document
    product = products_collection.find_one({'code': product_code})
    if not product:
        return jsonify({'message': False}), 404

    # Extract fields that can be updated
    product_type = data.get('type', product['type'])
    update_document = {}

    # Handle workwear product type
    if product_type == 'workwear':
        for field in ['employee', 'name', 'date', 'price', 'lifetime', 'residual_value', 'recommended_rest']:
            if field in data:
                update_document[field] = data.get(field)

        if 'employee' in update_document:
            update_document['subwarehouse'] = update_document['employee']

    # Handle distributor product type
    elif product_type == 'distributor':
        for field in ['distributor', 'name', 'amount', 'price', 'recommended_rest']:
            if field in data:
                update_document[field] = data.get(field)

        if 'amount' in update_document and 'price' in update_document:
            update_document['sum'] = float(update_document['amount']) * float(update_document['price'])
        elif 'amount' in update_document:
            update_document['sum'] = float(update_document['amount']) * product['price']
        elif 'price' in update_document:
            update_document['sum'] = product['amount'] * float(update_document['price'])

        if 'distributor' in update_document:
            update_document['subwarehouse'] = update_document['distributor']

        # Handle contract updates
        delete_contracts = request.form.getlist('delete_contracts')
        contracts = request.files.getlist('contracts')
        contracts_links_list = product.get('contracts_links', [])

        if delete_contracts:
            # Delete specified contracts
            for contract_link in contracts_links_list[:]:
                if contract_link in delete_contracts:
                    file_key = contract_link.split('/')[-1]
                    config.s3_client.delete_object(Bucket='olimpiabucket', Key=f'contracts_clients/{file_key}')
                    contracts_links_list.remove(contract_link)

        if contracts:
            # Upload new contracts
            for contract in contracts:
                unique_filename = generate_unique_filename(contract.filename)
                upload_contract_to_s3(contract, unique_filename)
                contracts_links_list.append(
                    f'https://olimpiabucket.fra1.digitaloceanspaces.com/contracts_clients/{unique_filename}')

        update_document['contracts_links'] = contracts_links_list

    elif product_type == '1c':
        for field in ['recommended_rest']:
            if field in data:
                update_document[field] = data.get(field)

    else:
        return jsonify({'message': False}), 400

    if update_document:
        products_collection.update_one({'code': product_code}, {'$set': update_document})

    return jsonify({'message': True}), 200


@application.route('/delete_product', methods=['POST'])
def delete_product():
    data = request.form
    product_id = data.get('product_id')
    access_token = data.get('access_token')

    if not check_token(access_token):
        return jsonify({'token': False}), 401

    # Find the product to delete
    product = products_collection.find_one({'_id': ObjectId(product_id)})
    if not product:
        return jsonify({'message': False}), 404

    # If the product has contracts, delete them from S3
    contracts_links = product.get('contracts_links', [])
    for contract_link in contracts_links:
        file_key = contract_link.split('/')[-1]
        try:
            config.s3_client.delete_object(Bucket='olimpiabucket', Key=f'contracts_clients/{file_key}')
        except Exception as e:
            print(f"Error deleting contract {file_key} from S3: {e}")

    # Delete the product from the database
    products_collection.delete_one({'_id': ObjectId(product_id)})

    return jsonify({'message': True}), 200


@application.route('/add_defective_product', methods=['POST'])
def add_defective_product():
    data = request.get_json()
    access_token = data.get('access_token')
    product_name = data.get('product_name')
    return_date = data.get('return_date')
    amount = data.get('amount')
    price = data.get('price')

    if not check_token(access_token):
        return jsonify({'token': False}), 401

    document = {'product_name': product_name,
                'return_date': return_date,
                'amount': amount,
                'price': price}

    defective_products_collection.insert_one(document)
    return jsonify({'message': True}), 200


@application.route('/update_defective_product', methods=['POST'])
def update_defective_product():
    data = request.get_json()
    access_token = data.get('access_token')

    if not check_token(access_token):
        return jsonify({'token': False}), 401

    product_id = data.get('product_id')
    product = defective_products_collection.find_one({'_id': ObjectId(product_id)})
    if product is None:
        return jsonify({'message': False}), 404

    product['product_name'] = data.get('product_name', product['product_name'])
    product['return_date'] = data.get('return_date', product['return_date'])
    product['amount'] = data.get('amount', product['amount'])
    product['price'] = data.get('price', product['price'])

    defective_products_collection.update_one({'_id': ObjectId(product_id)}, {'$set': product})
    return jsonify({'message': True}), 200


@application.route('/delete_defective_product', methods=['POST'])
def delete_defective_product():
    data = request.get_json()
    access_token = data.get('access_token')
    if check_token(access_token) is False:
        return jsonify({'token': False}), 401

    products_ids = data.get('products_ids')
    for product_id in products_ids:
        defective_products_collection.find_one_and_delete({'_id': ObjectId(product_id)})
    return jsonify({'message': True}), 200


@application.route('/add_pallet', methods=['POST'])
def add_pallet():
    data = request.get_json()
    access_token = data.get('access_token')
    counterpartie = data.get('counterpartie')
    amount = data.get('amount')
    price = data.get('price')

    if not check_token(access_token):
        return jsonify({'token': False}), 401

    document = {'counterpartie': counterpartie,
                'amount': amount,
                'price': price}

    defective_pallets_collection.insert_one(document)
    return jsonify({'message': True}), 200


@application.route('/update_pallet', methods=['POST'])
def update_pallet():
    data = request.get_json()
    access_token = data.get('access_token')

    if not check_token(access_token):
        return jsonify({'token': False}), 401

    pallet_id = data.get('pallet_id')
    pallet = defective_pallets_collection.find_one({'_id': ObjectId(pallet_id)})
    if pallet is None:
        return jsonify({'message': False}), 404

    pallet['counterpartie'] = data.get('counterpartie', pallet['counterpartie'])
    pallet['amount'] = data.get('amount', pallet['amount'])
    pallet['price'] = data.get('price', pallet['price'])

    defective_pallets_collection.update_one({'_id': ObjectId(pallet_id)}, {'$set': pallet})
    return jsonify({'message': True}), 200


@application.route('/delete_pallet', methods=['POST'])
def delete_pallet():
    data = request.get_json()
    access_token = data.get('access_token')
    if check_token(access_token) is False:
        return jsonify({'token': False}), 401

    pallets_ids = data.get('pallets_ids')
    for pallet_id in pallets_ids:
        defective_pallets_collection.find_one_and_delete({'_id': ObjectId(pallet_id)})
    return jsonify({'message': True}), 200


@application.route('/analytics', methods=['POST'])
def analytics():
    data = request.get_json()
    start_date = data.get('start_date')
    end_date = data.get('end_date')
    data_types = data.get('data_type', [])
    access_token = data.get('access_token')
    subwarehouse = data.get('subwarehouse')

    if not check_token(access_token):
        return jsonify({'token': False}), 401

    # Functions with and without date parameters
    functions_with_dates = {
        'total_sales': total_sales,
        'average_order_amount': average_order_amount,
        'order_volume_dynamic': order_volume_dynamic,
        'paid_orders_percentage': paid_orders_percentage,
        'repeat_orders_analyze': analyze_repeat_orders,
        'agents_rating': calculate_sales_agent_rating,
        'products_rating': calculate_product_rating,
        'total_amount_manufactured_by_good': get_total_amount_manufactured_by_good,
        'total_used_raw': get_total_used_raw,
        'defect_raw_percentage': get_defect_raw_percentage,
        'total_price_workwear': get_total_price_for_workwear,
        'total_contracts': get_contracts_stats,  # No need for lambda here
        'expiring_contracts': get_contracts_stats  # No need for lambda here
    }

    functions_without_dates = {
        'total_rest_by_warehouse': get_total_rest_by_warehouse,
        'low_stock_products': get_low_stock_products,
        'products_with_expired_series': get_products_with_expired_series,
        'total_amount_distributor': get_total_amount_for_distributor,
        'sale_products_report': sale_products_report,
        'defective_products_report': defective_products_report,
        'pallets_report': pallets_report
    }

    response_data = {}

    # Execute functions with date parameters
    for data_type in data_types:
        if data_type in functions_with_dates:
            func = functions_with_dates[data_type]
            result = func(start_date, end_date, subwarehouse)

            # Special handling for get_contracts_stats to extract correct value
            if data_type == 'total_contracts':
                result = result[0]
            elif data_type == 'expiring_contracts':
                result = result[1]

            response_data[data_type] = result

    # Execute functions without date parameters
    for data_type in data_types:
        if data_type in functions_without_dates:
            func = functions_without_dates[data_type]
            response_data[data_type] = func(subwarehouse)

    return jsonify(response_data), 200


from collections import defaultdict
from datetime import datetime


@application.route('/production', methods=['POST'])
def production():
    data = request.get_json()
    access_token = data.get('access_token')
    page = data.get('page', 1)
    per_page = data.get('per_page', 10)

    if not check_token(access_token):
        return jsonify({'token': False}), 401

    # Retrieve both manufactured products and used raw materials
    filter_criteria = {}
    manufactured_products = list(manufactured_products_collection.find(filter_criteria).sort('date', -1))
    used_raw_products = list(used_raw_collection.find(filter_criteria).sort('date', -1))

    # Merge the two datasets
    combined_products = manufactured_products + used_raw_products

    # Sort the combined list by date (assuming both have a 'date' field)
    combined_products.sort(key=lambda x: x.get('date', ''), reverse=True)

    # Group by date (day) using a defaultdict
    grouped_by_day = defaultdict(list)
    for document in combined_products:
        # Convert MongoDB ObjectId to string
        document['_id'] = str(document['_id'])

        # Assuming the date field is in a datetime format, we group by the day part
        date_str = document['date'].strftime('%Y-%m-%d')  # Format as 'YYYY-MM-DD'
        grouped_by_day[date_str].append(document)

    # Convert defaultdict back to a regular dictionary and sort by day (descending order)
    grouped_by_day = dict(sorted(grouped_by_day.items(), key=lambda x: x[0], reverse=True))

    # Implement pagination on the grouped results
    total_days = len(grouped_by_day)
    total_pages = math.ceil(total_days / per_page)

    # Extract only the required page of days
    skip = (page - 1) * per_page
    paginated_days = list(grouped_by_day.items())[skip:skip + per_page]

    # Calculate the range of days being displayed
    start_range = skip + 1
    end_range = min(skip + per_page, total_days)

    # Prepare the response
    response = Response(
        json_util.dumps({
            'products_by_day': paginated_days,  # Grouped data
            'total_days': total_days,
            'start_range': start_range,
            'end_range': end_range,
            'total_pages': total_pages
        }, ensure_ascii=False).encode('utf-8'),
        content_type='application/json;charset=utf-8'
    )

    return response


if __name__ == '__main__':
    application.run()

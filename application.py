import config
from flask import Flask, jsonify, request, Response
from flask_cors import CORS
from pymongo import MongoClient
import jwt
from bson import json_util, ObjectId
from flask_bcrypt import Bcrypt
from datetime import datetime, timedelta
import json
import math


application = Flask(__name__)
CORS(application)
application.config['SECRET_KEY'] = config.SECRET_KEY
SECRET_KEY = config.SECRET_KEY
client = MongoClient(config.MONGO_STRING)
db = client['size_crm']
users_collection = db['users']
statuses_collection = db['statuses']
tasks_collection = db['tasks']

bcrypt = Bcrypt(application)


@application.route('/', methods=['GET'])
def test():
    return 'OlimpiaCRM API v1.0'


def decode_access_token(access_token, secret_key):
    try:
        payload = jwt.decode(access_token, secret_key, algorithms=['HS256'])
        return payload
    except jwt.ExpiredSignatureError:
        # Handle expired token
        return None
    except jwt.InvalidTokenError:
        # Handle invalid token
        return None


def decode_refresh_token(refresh_token, secret_key):
    try:
        payload = jwt.decode(refresh_token, secret_key, algorithms=['HS256'])
        return payload
    except jwt.ExpiredSignatureError:
        # Handle expired token
        return None
    except jwt.InvalidTokenError:
        # Handle invalid token
        return None


# Sample function to verify access token
def verify_access_token(access_token):
    try:
        decoded_token = decode_access_token(access_token, SECRET_KEY)
        if decoded_token:
            user_id = decoded_token.get('user_id')
            # Fetch user data from the database using the user_id
            user = users_collection.find_one({'_id': ObjectId(user_id)})
            if user:
                name = user['name']
                userpic = user['userpic']
                return jsonify({'name': name, 'userpic': userpic}), 200
            # User is authenticated, proceed with processing the request
            else:
                return jsonify({'message': 'User not found'}), 404
        # User not found, handle the error
    except jwt.ExpiredSignatureError:
        # Token has expired
        return False
    except jwt.InvalidTokenError:
        # Invalid token
        return False


# Sample function to verify refresh token
def verify_refresh_token(refresh_token):
    try:
        decoded_token = decode_refresh_token(refresh_token, SECRET_KEY)
        if decoded_token:
            user_id = decoded_token.get('user_id')
            # Fetch user data from the database using the user_id
            user = users_collection.find_one({'_id': ObjectId(user_id)})
            if user:
                name = user['name']
                userpic = user['userpic']
                return jsonify({'name': name, 'userpic': userpic}), 200
            # User is authenticated, proceed with processing the request
            else:
                return jsonify({'message': 'User not found'}), 404
        # User not found, handle the error
    except jwt.ExpiredSignatureError:
        # Token has expired
        return False
    except jwt.InvalidTokenError:
        # Invalid token
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


# Endpoint for user login
@application.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    email = data.get('email')
    password = data.get('password')

    # Check if the user exists in the database
    user = users_collection.find_one({'email': email})

    if user:
        hashed_password_in_db = user.get('password', '')  # Assuming the field name is 'password'

        # Print or log the hashed password in the database for debugging
        #print(f"Hashed Password in DB: {hashed_password_in_db}")

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

    # Print or log the hashed password generated during login for debugging
    hashed_password_for_login = bcrypt.generate_password_hash(password).decode('utf-8')
    #print(f"Hashed Password for Login: {hashed_password_for_login}")

    response = jsonify({'message': 'Invalid credentials'}), 401
    return response


@application.route('/add_task', methods=['POST'])
def add_task():
    data = request.get_json()
    access_token = data.get('access_token')
    if check_token(access_token) is False:
        return jsonify({'token': False}), 401
    creator = data.get('creator')
    headline = data.get('headline')
    description = data.get('description', None)
    participants = data.get('participants', None)
    responsible = data.get('responsible', None)
    deadline = data.get('deadline', None)
    status = data.get('status', None)
    if status:
        status_doc = statuses_collection.find_one({'status': status})
        if status_doc:
            del status_doc['_id']
    comment = data.get('comment', None)

    # Get today's date
    today = datetime.today()
    # Format the date
    formatted_date = today.strftime("%a %b %d %Y")

    document = {'date': formatted_date,
                'creator': creator,
                'headline': headline,
                'description': description,
                'participants': participants,
                'responsible': responsible,
                'deadline': deadline,
                'status': status_doc,
                'comment': comment}
    tasks_collection.insert_one(document)
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

    # Update task fields based on the provided data
    task['headline'] = data.get('headline', task['headline'])
    task['creator'] = data.get('creator', task['creator'])
    task['description'] = data.get('description', task['description'])
    task['participants'] = data.get('participants', task['participants'])
    task['responsible'] = data.get('responsible', task['responsible'])
    deadline = data.get('deadline')
    if deadline:
        task['deadline'] = deadline
    status = data.get('status')
    if status:
        status_doc = statuses_collection.find_one({'status': status})
        if status_doc:
            del status_doc['_id']
        task['status'] = status_doc
    task['comment'] = data.get('comment', task['comment'])

    # Update the task in the database
    tasks_collection.update_one({'_id': ObjectId(task_id)}, {'$set': task})
    return jsonify({'message': True}), 200


@application.route('/delete_task', methods=['POST'])
def delete_task():
    data = request.get_json()
    access_token = data.get('access_token')
    if check_token(access_token) is False:
        return jsonify({'token': False}), 401

    task_id = data.get('task_id')
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
        # Convert ObjectId to string before returning the response
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
    page = data.get('page', 1)  # Default to page 1 if not provided
    per_page = data.get('per_page', 10)  # Default to 10 items per page if not provided

    filter_criteria = {}
    if keyword:
        tasks_collection.create_index([("$**", "text")])
        filter_criteria['$text'] = {'$search': keyword}

    # Count the total number of clients that match the filter criteria
    total_tasks = tasks_collection.count_documents(filter_criteria)

    total_pages = math.ceil(total_tasks / per_page)

    # Paginate the query results using skip and limit, and apply filters
    skip = (page - 1) * per_page
    documents = list(tasks_collection.find(filter_criteria).skip(skip).limit(per_page))
    for document in documents:
        document['_id'] = str(document['_id'])

    # Calculate the range of clients being displayed
    start_range = skip + 1
    end_range = min(skip + per_page, total_tasks)

    # Serialize the documents using json_util from pymongo and specify encoding
    response = Response(json_util.dumps(
        {'tasks': documents, 'total_tasks': total_tasks, 'start_range': start_range, 'end_range': end_range,
         'total_pages': total_pages},
        ensure_ascii=False).encode('utf-8'),
                        content_type='application/json;charset=utf-8')
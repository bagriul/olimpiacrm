import boto3

SECRET_KEY = 'olimpiacrmsecretkey'
MONGO_STRING = 'mongodb+srv://tsbgalcontract:mymongodb26@cluster0.kppkt.mongodb.net/test?authSource=admin&replicaSet=atlas-8jvx35-shard-0&readPreference=primary&appname=MongoDB%20Compass&ssl=true'

bot_token = '5196550429:AAEkRBGEgTodYlnI5jHBIqvcV5GWuXF7fuY'

ACCESS_ID = 'DO00LRM2FLWPZ4C2ZMPA'
SECRET_KEY = 'pBViFdsR5R8JCx7Dax/NDqvWDiJM/U3nTs+DXQY9F/4'
session = boto3.session.Session()
s3_client = session.client('s3', region_name='fra1', endpoint_url='https://fra1.digitaloceanspaces.com', aws_access_key_id=ACCESS_ID, aws_secret_access_key=SECRET_KEY)
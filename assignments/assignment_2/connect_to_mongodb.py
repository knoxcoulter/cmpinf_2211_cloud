import urllib.parse 
from pymongo import MongoClient  # <--- Add this line!

username = "kwc15"
password = urllib.parse.quote_plus("Mds_4204465@")
host = "mdsmongodb.sci.pitt.edu"
port = 27017
auth_db = "admin"  # or the database where the user is defined

# Format the connection string
uri = f"mongodb://{username}:{password}@{host}:{port}/?authSource={auth_db}"
mongo_client = MongoClient(uri)
mongo_db = mongo_client['emr_PITTID']
mongo_client.admin.command('ping')  # Trigger connection
print(":white_check_mark: Successfully connected to MongoDB with authentication.")
print("Available databases:", mongo_client.list_database_names())
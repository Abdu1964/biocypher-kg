from flask import Flask, request, jsonify
from pymongo import MongoClient
import urllib.parse
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)

# Get MongoDB credentials from environment
username = urllib.parse.quote_plus(os.getenv("MONGODB_USERNAME"))
password = urllib.parse.quote_plus(os.getenv("MONGODB_PASSWORD"))
cluster = os.getenv("MONGODB_CLUSTER")
appname = os.getenv("MONGODB_APPNAME")

# Construct the URI
uri = f"mongodb+srv://{username}:{password}@{cluster}/?retryWrites=true&w=majority&appName={appname}"

try:
    # Connect to MongoDB
    mongo_client = MongoClient(uri)
    db = mongo_client["output_wzp_all"]
    node_collections = [collection for collection in db.list_collection_names() if collection.startswith('nodes_')]
except Exception as e:
    print(f"Error connecting to MongoDB: {e}")
    node_collections = []
    

@app.route('/mongo', methods=['GET'])
def get_mongo_properties():
    node_id = request.args.get('id')
    if not node_id:
        return jsonify({"error": "Missing required parameter: id"}), 400
    
    properties = {}
    
    for collection_name in node_collections:
        try:
            collection = db[collection_name]
            node = collection.find_one({"id": node_id}, {"_id": 0})  # Exclude MongoDB _id
            if node:
                properties.update(node)
        except Exception as e:
            print(f"Error querying collection {collection_name}: {e}")
            continue  # Skip this collection and move to the next

    if properties:
        return jsonify(properties)
    else:
        return jsonify({"error": "Node not found or has no valid properties"}), 404

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
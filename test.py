from pymongo import MongoClient
import urllib.parse

# Replace with your MongoDB connection details
USERNAME = "root"
PASSWORD = "rootpassword"
HOST = "localhost"
PORT = "27017"
DATABASE_NAME = "elsevier"

# Properly encode the username and password
USERNAME = urllib.parse.quote_plus(USERNAME)
PASSWORD = urllib.parse.quote_plus(PASSWORD)

# Construct the MongoDB URI
MONGO_URI = f"mongodb://{USERNAME}:{PASSWORD}@{HOST}:{PORT}"

def query_large_collections(mongo_uri, database_name, batch_size):
    # Connect to the MongoDB client
    client = MongoClient(mongo_uri)

    try:
        # Access the database
        db = client[database_name]

        # Get all collection names
        collection_names = db.list_collection_names()
        print(f"Found {len(collection_names)} collections in the database '{database_name}': {collection_names}")

        # Query all documents from each collection in batches
        for collection_name in collection_names:
            print(f"\nProcessing collection: {collection_name}")
            collection = db[collection_name]

            cursor = collection.find().batch_size(batch_size)  # Set batch size for the cursor
            batch_count = 0

            for document in cursor:

                # Increment batch count and optionally log progress
                batch_count += 1
                if batch_count % batch_size == 0:
                    print(f"Processed {batch_count} documents from '{collection_name}'")

            print(f"Completed processing '{collection_name}' with {batch_count} documents.")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close the connection to the MongoDB server
        client.close()


# Run the script
if __name__ == "__main__":
    query_large_collections(MONGO_URI, DATABASE_NAME, 3)
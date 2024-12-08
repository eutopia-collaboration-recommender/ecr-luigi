#!/bin/bash
set -e

echo "Waiting for MongoDB to be ready..."
until mongo --username root --password rootpassword --eval "db.adminCommand('ping')"; do
  echo "MongoDB is not ready yet. Retrying in 2 seconds..."
  sleep 2
done

echo "MongoDB is ready. Restoring data..."

mongorestore --uri "mongodb://root:rootpassword@localhost:27017" --archive=/dump/mongodump.dump

echo "MongoDB restore completed successfully."
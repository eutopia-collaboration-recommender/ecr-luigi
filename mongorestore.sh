#!/bin/bash
set -e

echo "Waiting for MongoDB to be ready..."
until mongo --username $MONGO_INITDB_ROOT_USERNAME --password $MONGO_INITDB_ROOT_PASSWORD --eval "db.adminCommand('ping')"; do
  echo "MongoDB is not ready yet. Retrying in 2 seconds..."
  sleep 2
done

echo "MongoDB is ready. Restoring data..."
# Uncomment the following line to restore data from a dump
# mongorestore --uri "mongodb://$MONGO_INITDB_ROOT_USERNAME:$MONGO_INITDB_ROOT_PASSWORD@localhost:27017" --archive=/dump/mongodump.dump

echo "MongoDB restore completed successfully."
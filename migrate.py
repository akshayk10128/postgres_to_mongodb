import psycopg2
from pymongo import MongoClient
from pymongo import InsertOne, DeleteOne, ReplaceOne
from pymongo.errors import BulkWriteError
import datetime

date_format = "%d-%m-%Y"
mongo_conn = MongoClient('mongodb+srv://<username>:<password>@cluster1.d.mongodb.net?retryWrites=true') ## replace username and password with connection string
batch_size = 1000

## connect to Postgres
postgres_conn = psycopg2.connect(database="postgres",
                        host="localhost",
                        user="postgres",
                        password="password",
                        port="5432")

## Query to fetch rows from Postgres
def executePostgresQuery(limit):
    cursor = postgres_conn.cursor()
    cursor.execute("SELECT * FROM poccoll2 limit "+ str(limit))
    return cursor

## Query to fetch the count
def fetchCount():
    cursor = postgres_conn.cursor()
    cursor.execute("SELECT count(*) FROM poccoll2")
    return cursor.fetchone()

## MongoDB Connection with database
def mongoConnection():
    db = mongo_conn.databaseName ## replace databaseName with database name
    db_object = db["collectionName"] ## replace collectionName with collection name
    return db_object

## data parser
## update the below parser as per the data set
def dataParser(row):
    data = {}
    data["fld0"]= row[2]
    data["fld2"]= row[4]
    data["fld3"]= row[5]
    data["fld4"]= row[6]
    data["fld6"]= row[8]
    data["fld7"]= row[9]
    data["fld8"]= row[10]
    data["fld9"]= row[11]
    return data

number_of_documents = fetchCount()[0] ##fetches the number of documents to be migrated
## number_of_documents = 13619 --> For testing , hard code the limit

cursor = executePostgresQuery(number_of_documents) 

number_of_batches = number_of_documents // batch_size ## calculating number of batches for mongodb to ingest 1000 in one batch
remainder_docs = number_of_documents % batch_size
db_object = mongoConnection() ## connect to mongodb
batch = []
batch_number = 0
count = 0
start_time = datetime.datetime.now()
print ("Number of documents to be migrated "+ str(number_of_documents))
print("Inserting records with batch size of " + str(batch_size))
for row in cursor:   
    data = dataParser(row) ## transform data
    batch.append(InsertOne(data)) ## append to batch
    if len(batch) == batch_size: ## if batch contains data equivalent to batch size , eg 1000
        try:
            data_inserted = db_object.bulk_write(batch, ordered=False)
            batch_number = batch_number + 1
            count = count + batch_size
            print("Batch Number " + str(batch_number) + " inserted")
            batch = [] ## initialize batch size to 0 and empty
        except BulkWriteError as bwe:
            print(bwe.details)

    if number_of_batches == batch_number and len(batch) == remainder_docs : ## for remainining documents such as if 1397 documents , 397 will be inserted here
        print("Inserting delta batch of " + str(len(batch)))
        try:
            data_inserted = db_object.bulk_write(batch, ordered=False)
            count = count + len(batch)

        except BulkWriteError as bwe:
            print(bwe.details)

time_taken = datetime.datetime.now() - start_time 
print("Number of documents migrated : " + str(count))
print("Number of batches :- " + str(batch_number) + " --  Batch Size :- "+ str(batch_size) + " --  Time Taken :- "+ str(time_taken) )



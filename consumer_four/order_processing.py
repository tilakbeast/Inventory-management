import pika
import pymongo
import json
from bson import ObjectId


#connection = pika.BlockingConnection(pika.ConnectionParameters('localhost')) this is for running locally
credentials = pika.PlainCredentials(username='guest', password='guest')
parameters = pika.ConnectionParameters(host='rabbitmq', port=5672, credentials=credentials)
connection = pika.BlockingConnection(parameters=parameters)
channel = connection.channel()

# Declare the queue
channel.queue_declare(queue='order_processing_queue')
channel.queue_declare(queue='status')

# Set up MongoDB connections
#mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
inventory_mongo_client = pymongo.MongoClient("mongodb://database:27017/")
inventory_db = inventory_mongo_client["inventory"]
inventory_collection = inventory_db["items"]

#mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
orders_mongo_client = pymongo.MongoClient("mongodb://database:27017/")
orders_db = orders_mongo_client["orders"]
orders_collection = orders_db["orders"]

def send_status(message):
    channel.basic_publish(exchange='',
                          routing_key='status',
                          body=message)
    print("status updated")

# Define message handling function
def callback(ch, method, properties, body):
    # Convert the message body to a dictionary (assuming it's JSON)
    order_data = json.loads(body)
    name = order_data.get("name")
    #print(order_data,item_id)
    # Check if item exists in inventory and quantity > 0
    # print(inventory_collection.find())
    item = inventory_collection.find_one({"name": name, "quantity": {"$gt": 0}})
    
    if item:
        # Decrement quantity by 1
        inventory_collection.update_one({"name": name}, {"$inc": {"quantity": -1}})
        
        # Insert order into orders collection
        orders_collection.insert_one(order_data)
        
        print(" [x] Order processed and inserted into MongoDB: %r" % order_data)
        #send_status("Order processed and inserted into MongoDB")
    else:
        print(" [x] Item with ID %s is out of stock or does not exist in inventory. Order not processed." % item_id)
        #send_status("Item is out of stock or does not exist in inventory. Order not processed.")

# Set up message consumption
channel.basic_consume(queue='order_processing_queue',
                      on_message_callback=callback,
                      auto_ack=True)

# Start consuming messages
print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()

# Close connections
connection.close()
inventory_mongo_client.close()
orders_mongo_client.close()

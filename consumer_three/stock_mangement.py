import pika
import pymongo
import json
from bson import ObjectId


#connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
credentials = pika.PlainCredentials(username='guest', password='guest')
parameters = pika.ConnectionParameters(host='rabbitmq', port=5672, credentials=credentials)
connection = pika.BlockingConnection(parameters=parameters)
channel = connection.channel()


channel.queue_declare(queue='stock_management_queue')
channel.queue_declare(queue='status')


#mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
mongo_client = pymongo.MongoClient("mongodb://database:27017/")
db = mongo_client["inventory"]  
collection = db["items"]  

def send_status(message):
    channel.basic_publish(exchange='',
                          routing_key='status',
                          body=message)
    print(message)
    print("status updated")

def callback(ch, method, properties, body):
    # Convert the message body to a dictionary (assuming it's JSON)
    message_data = json.loads(body)
    
    # Extract item_id and new_stock_level from the message data
    name = message_data.get("name")
    new_stock_level = message_data.get("quantity")
    
    # Check if the item with the given item_id exists in the MongoDB collection
    existing_item = collection.find_one({"name": name})
    if existing_item:
        # Update stock level for the item in the MongoDB collection
        collection.update_one({"name": name}, {"$set": {"quantity": new_stock_level}})
        print(" [x] Stock level updated for item with name %s. New stock level: %d" % (name, new_stock_level))
        #send_status("stock updated!")
    else:
        # Item does not exist, print a message indicating it
        print(f"Item with name '{name}' does not exist in the MongoDB collection. Stock level not updated.")
        #send_status("Item does not exist, stock not updated!")


# Set up message consumption
channel.basic_consume(queue='stock_management_queue',
                      on_message_callback=callback,
                      auto_ack=True)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()

# Close connections
connection.close()
mongo_client.close()

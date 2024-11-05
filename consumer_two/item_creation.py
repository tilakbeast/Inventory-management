import pika
import pymongo
import json


#connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
credentials = pika.PlainCredentials(username='guest', password='guest')
parameters = pika.ConnectionParameters(host='rabbitmq', port=5672, credentials=credentials)
connection = pika.BlockingConnection(parameters=parameters)
channel = connection.channel()


channel.queue_declare(queue='item_creation_queue')
channel.queue_declare(queue='status')


#mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
mongo_client = pymongo.MongoClient("mongodb://database:27017/")
db = mongo_client["inventory"]  
collection = db["items"]  

def send_status(message):
    print(message)
    channel.basic_publish(exchange='',
                          routing_key='status',
                          body=message)
    print("status updated")


def callback(ch, method, properties, body):
    
    message_data = json.loads(body)
    item_name = message_data.get("name")
    message_data["_id"] = item_name
    

    existing_item = collection.find_one({"_id": item_name})
    if existing_item:
        print(f"Item '{item_name}' already exists in the MongoDB collection. Ignoring.")
        #send_status("Item already exists.")
    else:

        collection.insert_one(message_data)
        print(" [x] Item created and inserted into MongoDB: %r" % message_data)
        #send_status("Item created and inserted into inventory.")


channel.basic_consume(queue='item_creation_queue',
                      on_message_callback=callback,
                      auto_ack=True)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()

# Close connections
connection.close()
mongo_client.close()

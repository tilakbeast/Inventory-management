import pika
import json

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='health_check_queue')
channel.queue_declare(queue='item_creation_queue')
channel.queue_declare(queue='stock_management_queue')
channel.queue_declare(queue='order_processing_queue')

def produce_message(message):
    channel.basic_publish(exchange='',
                          routing_key='health_check_queue',
                          body=message)
    print(" [x] Sent %r" % message)

def create_item(message):
    channel.basic_publish(exchange='',
                          routing_key='item_creation_queue',
                          body=json.dumps(message))
    print(" [x] Sent %r" % message)

def update_stock(message):
    channel.basic_publish(exchange='',
                          routing_key='stock_management_queue',
                          body=json.dumps(message))
    print(" [x] Sent %r" % message)

def place_order(message):
    channel.basic_publish(exchange='',
                          routing_key='order_processing_queue',
                          body=json.dumps(message))
    print(" [x] Sent %r" % message)

produce_message('Health check message 1')
produce_message('Health check message 2')

item1={
  "name": "item 1",
  "description": "Description for Item 1",
  "price": 10.99,
  "quantity": 100
}
item2={
  "name": "item 2",
  "description": "Description for Item 2",
  "price": 10.99,
  "quantity": 100
}

create_item(item1)
create_item(item2)

update_item1={
  "item_id": "item 1",
  "new_stock_level": 50
}

update_stock(update_item1)

order_item1={
  "item_id": "item 3",
  "customer_name": "Krish Shah",
  "quantity": 1
}

order_item2={'item_id': 'item 1', 'customer_name': 'Samar Pratap', 'quantity': 4}
place_order(order_item1)
place_order(order_item2)
# Close connection
connection.close()

import pika

# Establish connection to RabbitMQ server
#connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
credentials = pika.PlainCredentials(username='guest', password='guest')
parameters = pika.ConnectionParameters(host='rabbitmq', port=5672, credentials=credentials)
connection = pika.BlockingConnection(parameters=parameters)
channel = connection.channel()

# Declare the queue
channel.queue_declare(queue='health_check_queue')

def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)

# Set up message consumption
channel.basic_consume(queue='health_check_queue',
                      on_message_callback=callback,
                      auto_ack=True)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()

# Close connection
connection.close()

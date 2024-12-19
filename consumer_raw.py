import pika

connection_parameter=pika.ConnectionParameters(
    host="localhost",
    port=5672,
    credentials=pika.PlainCredentials(
        username="guest",
        password="guest"
    )
)
channel=pika.BlockingConnection(connection_parameter).channel()
channel.queue_declare(
    queue="data_queue",
    durable=True
)
channel.basic_consume(
    queue="data_queue",
    auto_ack=True,
    on_message_callback=lambda ch,method,props,body:print(body)
)
print("Listenning Rabbitmq on 15672")
channel.start_consuming()
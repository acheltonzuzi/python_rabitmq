import pika

class RabbitMQConsumer:
    def __init__(self, host, port, username, password, queue_name):
        """Inicializa o consumidor RabbitMQ."""
        self.queue_name = queue_name
        self.connection_parameters = pika.ConnectionParameters(
            host=host,
            port=port,
            credentials=pika.PlainCredentials(username=username, password=password)
        )
        self.connection = None
        self.channel = None

    def connect(self):
        """Estabelece a conexão e configura o canal."""
        self.connection = pika.BlockingConnection(self.connection_parameters)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name, durable=True)
        print(f"Conectado ao RabbitMQ - Fila: {self.queue_name}")

    def start_consuming(self):
        """Inicia o consumo de mensagens."""
        if not self.channel:
            raise Exception("Conexão não estabelecida. Chame 'connect()' primeiro.")
        
        def on_message_callback(ch, method, properties, body):
            """Callback para processar mensagens recebidas."""
            print(f"Mensagem recebida: {body.decode()}")

        self.channel.basic_consume(
            queue=self.queue_name,
            auto_ack=True,
            on_message_callback=on_message_callback
        )
        print("Aguardando mensagens... Pressione CTRL+C para sair.")
        self.channel.start_consuming()

    def close(self):
        """Fecha a conexão com o RabbitMQ."""
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            print("Conexão com RabbitMQ fechada.")

# Exemplo de uso
if __name__ == "__main__":
    consumer = RabbitMQConsumer(
        host="localhost",
        port=5672,
        username="guest",
        password="guest",
        queue_name="data_queue"
    )
    try:
        consumer.connect()
        consumer.start_consuming()
    except KeyboardInterrupt:
        print("Interrompido pelo usuário.")
    finally:
        consumer.close()

import pika

class RabbitMQPublisher:
    def __init__(self, host, port, username, password, queue_name):
        """Inicializa o publisher RabbitMQ."""
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

    def publish_message(self, message):
        """Publica uma mensagem na fila."""
        if not self.channel:
            raise Exception("Conexão não estabelecida. Chame 'connect()' primeiro.")
        self.channel.basic_publish(
            exchange='python_excahange',
            routing_key="",
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=2  # Torna a mensagem persistente
            )
        )
        print(f"Mensagem publicada: {message}")

    def close(self):
        """Fecha a conexão com o RabbitMQ."""
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            print("Conexão com RabbitMQ fechada.")

# Exemplo de uso
if __name__ == "__main__":
    publisher = RabbitMQPublisher(
        host="localhost",
        port=5672,
        username="guest",
        password="guest",
        queue_name="data_queue"
    )
    try:
        publisher.connect()
        while True:
            mensagem = input("Digite a mensagem para enviar (ou 'sair' para finalizar): ")
            if mensagem.lower() == 'sair':
                break
            publisher.publish_message(mensagem)
    except KeyboardInterrupt:
        print("Interrompido pelo usuário.")
    finally:
        publisher.close()

import pika
from .middleware import MessageMiddlewareCloseError, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError, MessageMiddlewareQueue, MessageMiddlewareExchange

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.name = queue_name
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=queue_name)

    def start_consuming(self, on_message_callback):
        try:
            def callback(ch, method, properties, body):
                on_message_callback(body, lambda: ch.basic_ack(delivery_tag=method.delivery_tag), lambda: ch.basic_nack(delivery_tag=method.delivery_tag))
            
            self.channel.basic_consume(queue=self.name, on_message_callback=callback)
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception as e:
            raise MessageMiddlewareMessageError() from e

    def stop_consuming(self):
        try:
            self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
    
    def send(self, message):
        try:
            self.channel.basic_publish(exchange='', routing_key=self.name, body=message)
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception as e:
            raise MessageMiddlewareMessageError() from e

    def close(self):
        try:
            self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError() from e
        

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self.name = exchange_name
        self.routing_keys = routing_keys
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=exchange_name, exchange_type='direct')

    def start_consuming(self, on_message_callback):
        try:
            result = self.channel.queue_declare(queue='', exclusive=True)
            if result is None:
                raise MessageMiddlewareMessageError("Failed to declare a queue for consuming messages.")
            queue_name = result.method.queue

            for routing_key in self.routing_keys:
                self.channel.queue_bind(exchange=self.name, queue=queue_name, routing_key=routing_key)

            def callback(ch, method, properties, body):
                on_message_callback(body, lambda: ch.basic_ack(delivery_tag=method.delivery_tag), lambda: ch.basic_nack(delivery_tag=method.delivery_tag))
            
            self.channel.basic_consume(queue=queue_name, on_message_callback=callback)
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception as e:
            raise MessageMiddlewareMessageError() from e
    
    def stop_consuming(self):
        try:
            self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()

    def send(self, message):
        try:
            for routing_key in self.routing_keys:
                self.channel.basic_publish(exchange=self.name, routing_key=routing_key, body=message)
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception as e:
            raise MessageMiddlewareMessageError() from e
    
    def close(self):
        try:
            self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError() from e

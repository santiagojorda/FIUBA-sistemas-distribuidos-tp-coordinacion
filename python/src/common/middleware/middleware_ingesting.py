import pika
import logging
from .middleware import (
    MessageMiddlewareExchange,
  MessageMiddlewareQueue, 
  MessageMiddlewareCloseError,
  MessageMiddlewareDisconnectedError,
  MessageMiddlewareMessageError
)

from .rabbitmq_base import RabbitMQBase

MAX_MESSAGES_PER_WORKER = 1

class MessageMiddlewareQueueRabbitMQ(RabbitMQBase, MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        super().__init__(host)
        self.queue_name = queue_name
        self.channel.queue_declare(queue=self.queue_name, durable=True)
        self.channel.basic_qos(prefetch_count=MAX_MESSAGES_PER_WORKER)

    def send(self, message):
        try:
            self.channel.basic_publish(
                exchange="",
                routing_key=self.queue_name,
                body=message,
                properties=pika.BasicProperties(delivery_mode=2),
            )
        except pika.exceptions.AMQPConnectionError as e:
            self._cleanup_resources()
            raise MessageMiddlewareDisconnectedError("Conexión perdida al enviar a la cola") from e
        except pika.exceptions.AMQPChannelError as e:
            self._cleanup_resources()
            raise MessageMiddlewareMessageError("Error de canal al enviar a la cola") from e
        except Exception as e:
            self._cleanup_resources()
            raise MessageMiddlewareMessageError("Error interno inesperado al enviar") from e

    def close(self):
        try: 
            logging.info("Closing connection to RabbitMQ")
            self._cleanup_resources()
        except Exception as e:
            raise MessageMiddlewareCloseError("Error al cerrar la conexión") from e 

    def start_consuming(self, on_message_callback):
        def internal_callback(ch, method, properties, body):
            on_message_callback(
                body,
                lambda: ch.basic_ack(delivery_tag=method.delivery_tag),
                lambda: ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            )

        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=internal_callback,
            auto_ack=False
        )
        try:
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            self._cleanup_resources()
            raise MessageMiddlewareDisconnectedError("Conexión perdida con RabbitMQ") from e
        except pika.exceptions.AMQPChannelError as e:
            self._cleanup_resources()
            raise MessageMiddlewareMessageError("Error en el canal de RabbitMQ") from e
        except Exception as e:
            self._cleanup_resources()
            raise MessageMiddlewareMessageError("Error interno inesperado") from e


class MessageMiddlewareExchangeRabbitMQ(RabbitMQBase, MessageMiddlewareExchange):

    def __init__(self, host, exchange_name, route_keys):
        super().__init__(host)
        self.exchange_name = exchange_name
        self.route_keys = route_keys

        self.channel.exchange_declare(
            exchange=self.exchange_name,
            exchange_type="topic",
            durable=True,
        )

        # Cola efímera para consumo; evita colisiones entre réplicas.
        result = self.channel.queue_declare(queue="", exclusive=True)
        self.consumer_queue_name = result.method.queue
        for route_key in self.route_keys:
            self.channel.queue_bind(
                exchange=self.exchange_name,
                queue=self.consumer_queue_name,
                routing_key=route_key,
            )
        self.channel.basic_qos(prefetch_count=MAX_MESSAGES_PER_WORKER)

    def send(self, message):
        try:
            for route_key in self.route_keys:
                self.channel.basic_publish(
                    exchange=self.exchange_name,
                    routing_key=route_key,
                    body=message,
                    properties=pika.BasicProperties(delivery_mode=2),
                )
        except pika.exceptions.AMQPConnectionError as e:
            self._cleanup_resources()
            raise MessageMiddlewareDisconnectedError("Conexión perdida al enviar al exchange") from e
        except pika.exceptions.AMQPChannelError as e:
            self._cleanup_resources()
            raise MessageMiddlewareMessageError("Error de canal al enviar al exchange") from e
        except Exception as e:
            self._cleanup_resources()
            raise MessageMiddlewareMessageError("Error interno inesperado al enviar") from e

    def close(self):
        try:
            logging.info("Closing connection to RabbitMQ")
            self._cleanup_resources()
        except Exception as e:
            raise MessageMiddlewareCloseError("Error al cerrar la conexión") from e

    def start_consuming(self, on_message_callback):
        def internal_callback(ch, method, properties, body):
            on_message_callback(
                body,
                lambda: ch.basic_ack(delivery_tag=method.delivery_tag),
                lambda: ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            )

        self.channel.basic_consume(
            queue=self.consumer_queue_name,
            on_message_callback=internal_callback,
            auto_ack=False,
        )
        try:
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            self._cleanup_resources()
            raise MessageMiddlewareDisconnectedError("Conexión perdida con RabbitMQ") from e
        except pika.exceptions.AMQPChannelError as e:
            self._cleanup_resources()
            raise MessageMiddlewareMessageError("Error en el canal de RabbitMQ") from e
        except Exception as e:
            self._cleanup_resources()
            raise MessageMiddlewareMessageError("Error interno inesperado") from e

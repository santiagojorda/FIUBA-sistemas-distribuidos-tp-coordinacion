import logging

import pika

from .middleware import (
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareQueue,
)
from .rabbitmq_base import RabbitMQBase

MAX_MESSAGES_PER_WORKER = 1
EXCHANGE_NAME = "sum_control_exchange"
QUEUE_NAME_PREFIX = "sum_control_queue"


class MessageMiddlewareSumWorkerControlQueue(RabbitMQBase, MessageMiddlewareQueue):
    def __init__(self, host, worker_id):
        super().__init__(host)

        self.exchange_name = EXCHANGE_NAME
        self.queue_name = f"{QUEUE_NAME_PREFIX}_{worker_id}"

        self.channel.exchange_declare(
            exchange=self.exchange_name,
            exchange_type="fanout",
            durable=True,
        )
        self.channel.queue_declare(queue=self.queue_name, durable=True)
        self.channel.queue_bind(exchange=self.exchange_name, queue=self.queue_name)
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
            raise MessageMiddlewareDisconnectedError("Conexión perdida al enviar al middleware de control") from e
        except pika.exceptions.AMQPChannelError as e:
            self._cleanup_resources()
            raise MessageMiddlewareMessageError("Error de canal al enviar al middleware de control") from e
        except Exception as e:
            self._cleanup_resources()
            raise MessageMiddlewareMessageError("Error interno inesperado al enviar") from e

    def start_consuming(self, on_message_callback):
        def internal_callback(ch, method, properties, body):
            on_message_callback(
                body,
                lambda: ch.basic_ack(delivery_tag=method.delivery_tag),
                lambda: ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True),
            )

        try:
            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=internal_callback,
                auto_ack=False,
            )
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


class MessageMiddlewareSumWorkerControlExchange(RabbitMQBase, MessageMiddlewareExchange):
    def __init__(self, host):
        super().__init__(host)

        self.exchange_name = EXCHANGE_NAME
        self.consumer_queue_name = None

        self.channel.exchange_declare(
            exchange=self.exchange_name,
            exchange_type="fanout",
            durable=True,
        )

    def send(self, message):
        try:
            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key="",
                body=message,
                properties=pika.BasicProperties(delivery_mode=2),
            )
        except pika.exceptions.AMQPConnectionError as e:
            self._cleanup_resources()
            raise MessageMiddlewareDisconnectedError("Conexión perdida al publicar el mensaje de control") from e
        except pika.exceptions.AMQPChannelError as e:
            self._cleanup_resources()
            raise MessageMiddlewareMessageError("Error de canal al publicar el mensaje de control") from e
        except Exception as e:
            self._cleanup_resources()
            raise MessageMiddlewareMessageError("Error interno inesperado al publicar") from e

    def start_consuming(self, on_message_callback):
        def internal_callback(ch, method, properties, body):
            on_message_callback(
                body,
                lambda: ch.basic_ack(delivery_tag=method.delivery_tag),
                lambda: ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True),
            )

        try:
            result = self.channel.queue_declare(queue="", exclusive=True)
            self.consumer_queue_name = result.method.queue
            self.channel.queue_bind(
                exchange=self.exchange_name,
                queue=self.consumer_queue_name,
            )
            self.channel.basic_qos(prefetch_count=MAX_MESSAGES_PER_WORKER)
            self.channel.basic_consume(
                queue=self.consumer_queue_name,
                on_message_callback=internal_callback,
                auto_ack=False,
            )
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
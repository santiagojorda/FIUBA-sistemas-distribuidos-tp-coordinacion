
import pika
from .middleware import (
    MessageMiddlewareCloseError, 
    MessageMiddlewareDisconnectedError, 
    MessageMiddlewareMessageError
)

class RabbitMQBase:
    def __init__(self, host):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def _cleanup_resources(self):
        if self.channel and self.channel.is_open:
            try:
                self.channel.close()
            except Exception:
                pass
        if self.connection and self.connection.is_open:
            try:
                self.connection.close()
            except Exception:
                pass
        self.channel = None
        self.connection = None

    def stop_consuming(self):
        try:
            if self.channel:
                self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            self._cleanup_resources()
            raise MessageMiddlewareDisconnectedError("Conexión perdida con RabbitMQ") from e
        except pika.exceptions.AMQPChannelError as e:
            self._cleanup_resources()
            raise MessageMiddlewareMessageError("Error en el canal de RabbitMQ") from e
        except Exception as e:
            self._cleanup_resources()
            raise MessageMiddlewareMessageError("Error interno inesperado") from e

    def close(self):
        try:
            self._cleanup_resources()
        except Exception as e:
            raise MessageMiddlewareCloseError("Error al cerrar la conexión") from e


import time
import json
from logging import getLogger
from threading import Lock
from urllib.parse import urlparse

import pika
import xxhash
from pika import exceptions as pika_exceptions


logger = getLogger()


class QueueError(Exception):
    pass


class Queue(object):

    def __init__(self, connection_url, target_queue, task_worst_time_secs):
        """
        :param connection_url: Rabbitmq connection URL
        :param target_queue: Name of the message queue
        :param task_worst_time_secs: Worst case time which the task can take

        :Example:

        url = "amqp://username:password@host:port/virtual_host"
        queue = "my_message_queue"
        q = Queue(url, queue, task_worst_time_secs=30)

        """
        self._channel = None
        self._connection = None
        self._queue = target_queue
        self._task_worst_time_secs = task_worst_time_secs
        self._rabbitmq_msg_properties = {}
        self._queue_consumer = None
        self._queue_consumption_lock = Lock()
        self._put_lock = Lock()
        self._last_msg_seen_delivery_tag = None
        self._last_msg_seen_hash = None
        self._last_processed_msg_ack_failed = False
        self._rabbitmq_connection_url = self._convert_to_rabbitmq_connection_url(connection_url)
        self._setup_rabbitmq()

    def __del__(self):
        if self._connection and self._connection.is_open:
            logger.info("Closing Rabbitmq Connection")
            self._connection.close()

    def _setup_rabbitmq(self):
        SUCCESS = False
        connection_retry_interval = 2  # seconds
        while not SUCCESS:
            try:
                # Terminating existing connection if any
                if self._connection and self._connection.is_open:
                    logger.info("Closing existing rabbitmq connection")
                    self._connection.close()

                # Releasing queue consumption lock because of
                # avoiding sending msg ack to new channel
                if self._queue_consumption_lock.locked():
                    self._queue_consumption_lock.release()
                    self._last_msg_seen_delivery_ack = None

                # Creating new rabbitmq connection
                logger.info("Establishing connection with Rabbitmq")
                parameters = pika.URLParameters(self._rabbitmq_connection_url)
                self._connection = pika.BlockingConnection(parameters)

                # Channel Setup
                logger.info("Creating channel in rabbitmq connection")
                self._channel = self._connection.channel()
                self._channel.basic_qos(prefetch_count=1) # Consume only one msg at a time

                # Declaring Queue
                self._declare_queue(self._queue)

                # Callbacks for handling rabbitmq's resource exhaustion
                self._connection.add_on_connection_blocked_callback(self._disable_put)
                self._connection.add_on_connection_unblocked_callback(self._enable_put)

                # Starting queue consumer
                self._queue_consumer = self._start_queue_consumer()

                SUCCESS = True
                logger.info("Successfully completed setup of rabbitmq")

            except (pika_exceptions.AMQPConnectionError,
                    pika_exceptions.ChannelClosed,
                    pika_exceptions.StreamLostError) as err:
                error_msg = str(err)
                logger.error(error_msg)

                if connection_retry_interval > 100:
                    connection_retry_interval = 2
                else:
                    connection_retry_interval *= 2

                logger.info("Will try after {} seconds".format(
                            connection_retry_interval))
                time.sleep(connection_retry_interval)

    def _disable_put(self, method):
        self._put_lock.acquire()

    def _enable_put(self, method):
        self._put_lock.release()

    def _set_rabbitmq_msg_properties(self):
        pass

    def _declare_queue(self, queue):
        if self._channel:
            if not self._queue:
                raise QueueError("Please provide the target queue name")
            self._channel.queue_declare(queue, durable=True)

    def _convert_to_rabbitmq_connection_url(self, url):
        parsed_url = urlparse(url)

        if not parsed_url.path:
            raise QueueError("Please provide the path to virtual host")

        # Adding %2F(/) to path of url
        rabbitmq_url = "{scheme}://{netloc}/%2F{path}?{query_params}".format(
            scheme=parsed_url.scheme, netloc=parsed_url.netloc,
            path=parsed_url.path[1:],
            query_params="heartbeat={}".format(self._task_worst_time_secs)
        )
        return rabbitmq_url

    def _start_queue_consumer(self):
        while True:
            try:
                for method, properties, body in self._channel.consume(self._queue):
                    if self._last_processed_msg_ack_failed and method.redelivered:
                        current_msg_hash = xxhash.xxh64(body).hexdigest()
                        if self._last_msg_seen_hash == current_msg_hash:
                            logger.info("Got already processed msg!")
                            if not self._queue_consumption_lock.locked():
                                self._queue_consumption_lock.acquire()

                            self._last_msg_seen_delivery_tag = method.delivery_tag
                            self.task_done()

                            logger.info("Rejected!")
                            logger.info("Getting next message")
                            continue
                    yield method, properties, body

            except (pika_exceptions.AMQPConnectionError,
                    pika_exceptions.ChannelClosed,
                    pika_exceptions.StreamLostError) as err:
                error_msg = str(err)
                logger.error(error_msg)
                self._setup_rabbitmq()

    def _get_msg_properties(self):
        properties = pika.BasicProperties(**self._rabbitmq_msg_properties)
        return properties

    def _get_msg_from_queue(self):
        if self._queue_consumer is None:
            self._queue_consumer = self._start_queue_consumer()

        method, properties, body = self._queue_consumer.__next__()
        if not self._queue_consumption_lock.locked():
            self._queue_consumption_lock.acquire()

        return method.delivery_tag, body

    def get(self):
        self._queue_consumption_lock.acquire()
        delivery_tag, msg = self._get_msg_from_queue()
        self._last_msg_seen_delivery_tag = delivery_tag
        self._last_msg_seen_hash = xxhash.xxh64(msg).hexdigest()
        return msg

    def put(self, msg, rabbitmq_msg_properties={}):
        if self._put_lock.locked():
            raise QueueError("Put operation is disabled")

        msg_properties = self._get_msg_properties()

        if rabbitmq_msg_properties:
            for _property in rabbitmq_msg_properties:
                _property_value = rabbitmq_msg_properties[_property]
                setattr(msg_properties, _property, _property_value)

        if type(msg) == dict:
            msg = json.dumps(msg)

        SUCCESS = False
        while not SUCCESS:
            try:
                self._channel.basic_publish(
                    exchange="", routing_key=self._queue,
                    body=msg, properties=msg_properties
                )
                SUCCESS = True

            except (pika_exceptions.AMQPConnectionError,
                    pika_exceptions.ChannelClosed,
                    pika_exceptions.StreamLostError) as err:
                error_msg = str(err)
                logger.error(error_msg)
                self._setup_rabbitmq()

    def task_done(self):
        try:
            if not self._queue_consumption_lock.locked():
                return

            if self._last_msg_seen_delivery_tag is not None:
                logger.info("Sending ack: {} to queue: {}".format(
                            self._last_msg_seen_delivery_tag, self._queue))
                self._channel.basic_ack(self._last_msg_seen_delivery_tag)

            self._last_processed_msg_ack_failed = False
            self._queue_consumption_lock.release()

        except (pika_exceptions.AMQPConnectionError,
                pika_exceptions.ChannelClosed,
                pika_exceptions.StreamLostError) as err:
            error_msg = str(err)
            logger.error(error_msg)
            self._last_processed_msg_ack_failed = True
            self._setup_rabbitmq()


class PersistentQueue(Queue):

    def __init__(self, connection_url, target_queue, task_worst_time_secs):
        super(PersistentQueue, self).__init__(
              connection_url, target_queue, task_worst_time_secs)
        self._rabbitmq_msg_properties["delivery_mode"] = 2


class PriorityQueue(Queue):

    def __init__(self, connection_url, target_queue,
                 task_worst_time_secs, default_priority=5):
        super(PriorityQueue, self).__init__(
              connection_url, target_queue, task_worst_time_secs)
        self.max_queue_priority = 10
        self.default_priority = default_priority
        self._rabbitmq_msg_properties["priority"] = default_priority

    def _declare_queue(self, queue):
        if self._channel:
            if not self._queue:
                raise QueueError("Please provide the target queue name")
            queue_properties = {"x-max-priority": 10}
            self._channel.queue_declare(
                queue,
                durable=True,
                arguments=queue_properties
            )

    def put(self, msg, priority=None):
        if not priority:
            priority = self.default_priority
        super(PriorityQueue, self).put(
              msg, rabbitmq_msg_properties={"priority": priority})


class PersistentPriorityQueue(PriorityQueue, PersistentQueue):

    def __init__(self, connection_url, target_queue,
                 task_worst_time_secs, default_priority=5):
        super(PersistentPriorityQueue, self).__init__(
            connection_url,
            target_queue,
            task_worst_time_secs,
            default_priority=default_priority
        )

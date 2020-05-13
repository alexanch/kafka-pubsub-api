import os
from google.cloud import pubsub_v1
from kafka import KafkaProducer, KafkaConsumer
import json
from google.auth import jwt
import _pickle

"""
This is a library for Apache Kafka or Google Pub/Sub.
"""

class CloudPubSub:
    def __init__(self, topic='test', project_id='b-fashion',
                 path="C:/Users/Alexanch/Desktop/VECTOR AI/kafka-pubsub-api/key.json"):
        """
        :param project_id: project ID
        :param topic: Topic name
        :param path: path to JSON Web Token
        """
        self.topic = topic
        self.project_id = project_id
        self.path = path
        # auth
        self.auth()

    def auth(self):
        """
        Authentication method to use with the Pub/Sub clients using JSON Web Tokens
        """
        # set os environment's path to JSON key
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.path
        #######
        try:
            service_account_info = json.load(open(self.path))
            audience = "https://pubsub.googleapis.com/google.pubsub.v1.Subscriber"

            credentials = jwt.Credentials.from_service_account_info(
                service_account_info, audience=audience
            )

            subscriber = pubsub_v1.SubscriberClient(credentials=credentials)

            # The same for the publisher, except that the "audience" claim needs to be adjusted
            publisher_audience = "https://pubsub.googleapis.com/google.pubsub.v1.Publisher"
            credentials_pub = credentials.with_claims(audience=publisher_audience)
            publisher = pubsub_v1.PublisherClient(credentials=credentials_pub)
            print('Auth is successful')
        except:
            print('Auth failed.')

    def pub(self, data=b'Hey VectorAI!'):
        """
        Publish request to Google Pub/Sub
        Reference: https://googleapis.dev/python/pubsub/latest/publisher/index.html
        :param data: anything (binary data) to be sent to Pub/Sub
        """
        publisher = pubsub_v1.PublisherClient()
        topic_name = f'projects/{self.project_id}/topics/{self.topic}'
        # publisher.create_topic(topic_name)
        #  to include attributes, simply add keyword arguments: e.g., spam ='eggs'
        future = publisher.publish(topic_name, data)  # spam='eggs'
        # to check if the publish succeded:
        print(future.result())
        print("Published messages.")

    def sub(self, sub_name='test_sub'):
        """
        Subscribe to topic to Google Pub/Sub
        Reference: https://googleapis.dev/python/pubsub/latest/subscriber/index.html
        :param sub_name: subscription name
        """
        subscriber = pubsub_v1.SubscriberClient()
        # Substitute PROJECT, SUBSCRIPTION, and TOPIC with appropriate values for
        # your application.
        topic_name = f'projects/{self.project_id}/topics/{self.topic}'
        subscription_name = f'projects/{self.project_id}/subscriptions/{sub_name}'

        try:
            subscriber.create_subscription(
                name=subscription_name, topic=topic_name)
        except:
            print(f'{subscription_name} exists.')

        def callback(message):
            print("Received message: {}".format(message.data))
            message.ack()

        future = subscriber.subscribe(subscription_name, callback)
        # Wrap subscriber in a 'with' block to automatically call close() when done.
        with subscriber:
            try:
                # When `timeout` is not set, result() will block indefinitely,
                # unless an exception is encountered first.
                future.result()
            except:
                future.cancel()


class ApacheKafka:
    def __init__(self, topic='MY_TOPIC_NAME', bootstrap_servers='localhost:9092'):
        """
        :param topic: topic name :param bootstrap_servers: 'host[:port]' string (or list of 'host[:port]' strings) 
        that the consumer should contact to bootstrap initial cluster metadata.
        """
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers

    def kafka_producer(self, data):
        """
        Kafka Producer
        Reference: https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html
        :param data: anything (binary data) to be sent to Kafka
        """
        # We then grab the URL of our broker. Producers use that URL to bootstrap their connection to the Kafka cluster.
        # KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
        # producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER_URL)
        producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers)
        producer.send(self.topic, data)
        # producer.send(topic, key=b'message-two', value=b'This is Kafka-Python')

    def kafka_consumer(self, sub_name='MY_SUBSCRIPTION_NAME'):
        """
        Kafka Consumer
        Reference: https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html
        """
        consumer = KafkaConsumer(self.topic, bootstrap_servers=self.bootstrap_servers)
        for message in consumer:
            print(message)


class Api:
    """
    Unified API: you can choose between Cloud PubSub or Apache Kafka. Pass the params accordingly.
    Cloud PubSub requires: topic, project_id, path to auth key
    Apache Kafka requires: topic, bootstrap_servers
    """
    def __init__(self, api_type, topic='test', path="key.json",
                 project_id='b-fashion', bootstrap_servers='localhost:9092', *args):
        self.api_type = api_type
        self.topic = topic
        self.path = path
        self.project_id = project_id
        self.bootstrap_servers = bootstrap_servers

    def select(self):
        if self.api_type == 'CloudPubSub':
            return CloudPubSub(topic='test', path="C:/Users/Alexanch/Desktop/VECTOR AI/kafka-pubsub-api/key.json",
                               project_id='b-fashion')
        elif self.api_type == 'ApacheKafka':
            return ApacheKafka(topic='test', bootstrap_servers='localhost:9092')
        else:
            print('Choose between CloudPubSub or ApacheKafka.')


#####################################

"""
- A publisher sends a message.
- The message is written to storage.
- Pub/Sub sends an acknowledgement to the publisher that it has received the message and guarantees its delivery to all attached subscriptions.
- At the same time as writing the message to storage, Pub/Sub delivers it to subscribers.
- Subscribers send an acknowledgement to Pub/Sub that they have processed the message.
- Once at least one subscriber for each subscription has acknowledged the message, Pub/Sub deletes the message from storage.

The system is designed to be horizontally scalable,
where an increase in the number of topics, subscriptions,
or messages can be handled by increasing the number of instances of running servers.
"""

# Publishers can be any application that can make HTTPS requests to pubsub.googleapis.com:
# an App Engine app, a web service hosted on Google Compute Engine or any other third-party network,
# an app installed on a desktop or mobile device, or even a browser.

# optional attributes


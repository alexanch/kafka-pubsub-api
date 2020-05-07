import os
from google.cloud import pubsub_v1
from kafka import KafkaProducer, KafkaConsumer

"""
This is a library for Apache Kafka or Google Pub/Sub
"""
# There are several options to create Producer And Consumer for Apache Kafka:
    # Command line client provided as default by Kafka
    # kafka-python (chosen one)
    # PyKafka
    # confluent-kafka

def kafka_producer(topic = 'MY_TOPIC_NAME', data = b'Hey, VectorAI!', bootstrap_servers='localhost:9092'):
    """
    Kafka Producer
    Reference: https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html
    :param topic: topic name
    :param data: anything (binary data) to be sent to Kafka
    :param bootstrap_servers: 'host[:port]' string (or list of 'host[:port]' strings) that the consumer should contact to bootstrap initial cluster metadata.
    """

    # We then grab the URL of our broker. Producers use that URL to bootstrap their connection to the Kafka cluster.
    # KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
    # producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER_URL)

    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer.send(topic, data)
    # producer.send(topic, key=b'message-two', value=b'This is Kafka-Python')


def kafka_consumer(topic = 'MY_TOPIC_NAME', bootstrap_servers='localhost:9092'):
    """
    Kafka Consumer
    Reference: https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html
    :param topic: topic name
    :param bootstrap_servers: 'host[:port]' string (or list of 'host[:port]' strings) that the consumer should contact to bootstrap initial cluster metadata.
    """

    consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers)
    for message in consumer:
        print (message)


def google_pub(topic='MY_TOPIC_NAME', data = b'Hey VectorAI!', project_id='GOOGLE_CLOUD_PROJECT'):
    """
    Publish request to Google Pub/Sub
    Reference: https://googleapis.dev/python/pubsub/latest/publisher/index.html
    :param data: anything (binary data) to be sent to Pub/Sub
    :param project_id: project ID
    :param topic: Topic name
    """

    project_id = os.getenv(project_id)
    publisher = pubsub_v1.PublisherClient()
    topic_name = f'projects/{project_id}/topics/{topic}'

    publisher.create_topic(topic_name)
    #  to include attributes, simply add keyword arguments: e.g., spam ='eggs'
    publisher.publish(topic_name, data, spam='eggs')


def google_sub(topic='MY_TOPIC_NAME', project_id=os.getenv('GOOGLE_CLOUD_PROJECT'), sub='MY_SUBSCRIPTION_NAME'):
    """
    Subscribe to topic to Google Pub/Sub
    Reference: https://googleapis.dev/python/pubsub/latest/subscriber/index.html
    :param project_id: project ID
    :param topic: Topic name
    :param sub: subscription name
    """

    subscriber = pubsub_v1.SubscriberClient()
    # Substitute PROJECT, SUBSCRIPTION, and TOPIC with appropriate values for
    # your application.
    topic_name = f'projects/{project_id}/topics/{topic}'
    subscription_name = f'projects/{project_id}/subscriptions/{sub}'

    subscriber.create_subscription(
        name=subscription_name, topic=topic_name)

    def callback(message):
        print(message.data)
        message.ack()

    future = subscriber.subscribe(subscription_name, callback)
    try:
        future.result()
    except KeyboardInterrupt:
        future.cancel()


def google_auth(path,name="service-account-info.json"):
    """
    Authentication method to use with the Pub/Sub clients using JSON Web Tokens
    :param path: path to JSON Web Token
    :param name: JSON Token name
    :return:
    """
    import json
    from google.auth import jwt

    service_account_info = json.load(open(path+name))
    audience = "https://pubsub.googleapis.com/google.pubsub.v1.Subscriber"

    credentials = jwt.Credentials.from_service_account_info(
        service_account_info, audience=audience
    )

    subscriber = pubsub_v1.SubscriberClient(credentials=credentials)

    # The same for the publisher, except that the "audience" claim needs to be adjusted
    publisher_audience = "https://pubsub.googleapis.com/google.pubsub.v1.Publisher"
    credentials_pub = credentials.with_claims(audience=publisher_audience)
    publisher = pubsub_v1.PublisherClient(credentials=credentials_pub)


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

# API(method=pub/sub, app=kafka/gcp, topic='', message='', request_type=JSON/REST)

# Publishers can be any application that can make HTTPS requests to pubsub.googleapis.com:
# an App Engine app, a web service hosted on Google Compute Engine or any other third-party network,
# an app installed on a desktop or mobile device, or even a browser.

# check for errors
# optional attributes
# convert to bytes before/after?
# print a message about the result to console


####### get message
# input: topic, method = pushing/pulling

# Pull subscribers can also be any application that can make HTTPS requests to pubsub.googleapis.com.
# Push subscribers must be Webhook endpoints that can accept POST requests over HTTPS.


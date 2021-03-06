# kafka-pubsub-api

### Unified API to send/receive requests from Apache Kafka/Google Pub/Sub 

### Client libraries used in this API: 
- [[Python Client for Google Cloud Pub / Sub]](https://googleapis.dev/python/pubsub/latest/index.html)
- [[kafka-python API]](https://kafka-python.readthedocs.io/en/master/apidoc/modules.html)



### How to use:

1. Git clone the repo:
    ```
    git clone https://github.com/alexanch/kafka-pubsub-api.git
    ```
2. change dir: 
    ```
    cd kafka-pubsub-api
    ```
1. in terminal, write:
    ```
    pip install -r requirements.txt
    ```
2. launch iPython in terminal:
    ```
    ipython
    ```
3. Import API module:
    ```
    import module
    ```
4. Load API of choise: 

   <b>Cloud Pub/Sub:</b>
   ```
   # Api arguments: api_type, topic, path to JSON key for authorization, project_id (for PubSub)
   pubsub = module.Api('CloudPubSub').select()   
   
   # methods:
   pubsub.pub(data = b'data') # any binary data can be passed
   pubsub.sub(sub_name) # pass subscriber's name
   
    ```
   <b>Apache Kafka:</b>
    ```
   # Api arguments: api_type, topic, bootstrap_servers (for Kafka)
   kafka = module.Api('ApasheKafka').select()   
   
   # methods:
   kafka.pub(data = b'data') # any binary data can be passed
   kafka.sub(sub_name) # pass subscriber's name
   ```
 

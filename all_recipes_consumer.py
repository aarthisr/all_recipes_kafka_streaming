import json, sys
from time import sleep
from kafka import KafkaConsumer, KafkaProducer
import re


def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer


def parse(markup):
    title = '-'
    submit_by = '-'
    description = '-'
    calories = 0
    rec = {}
    try:
        title = re.search('property="og:title" content="(.*?)">',str(markup)).group(1)
        submit_by = re.search('<a class="author-name link" href=".*?">(.*?)</a>',str(markup)).group(1)
        description = re.search('"description": "(.*?)",',str(markup)).group(1)
        calories=re.search('"calories": "(.*?) calories",',str(markup)).group(1)
        rec = {'title': title, 'submitter': submit_by, 'description': description, 'calories': calories}
    except Exception as ex:
        print('Exception while parsing')
        print(str(ex))
        sys.exit()
    finally:
        return json.dumps(rec)


if __name__ == '__main__':
    print('Running Consumer..')
    parsed_records = []
    topic_name = 'all_recipes'
    parsed_topic_name = 'ext_recipes'

    consumer = KafkaConsumer(topic_name, auto_offset_reset='earliest',
                             bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)
    for msg in consumer:
        html = msg.value
        # print(html)
        result = parse(html)
        parsed_records.append(result)
    consumer.close()
    sleep(5)

    if len(parsed_records) > 0:
        print('Publishing records..')
        producer = connect_kafka_producer()
        for rec in parsed_records:
            publish_message(producer, parsed_topic_name, 'parsed', rec)

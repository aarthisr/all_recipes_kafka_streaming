import json
from time import sleep

from kafka import KafkaConsumer

if __name__ == '__main__':
    parsed_topic_name = 'ext_recipes'
    # Notify if a recipe has more than 100 calories
    calories_threshold = 100

    consumer = KafkaConsumer(parsed_topic_name, auto_offset_reset='earliest',
                             bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)
    for msg in consumer:
        record = json.loads(msg.value)
        calories = float(record['calories'])
        title = record['title']

        if calories > calories_threshold:
            print('Alert: {} calories count is {}'.format(title, calories))
        elif calories == 0:
            print('Alert: no calories count for {}'.format(title))
        sleep(3)

    if consumer is not None:
        consumer.close()
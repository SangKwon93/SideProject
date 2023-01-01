from kafka import KafkaConsumer,KafkaProducer
import json


TARGET_TOPIC= 'target'
BROKERS = ["localhost:8900", "localhost:8901", "localhost:8902"]
consumer = KafkaConsumer(TARGET_TOPIC, bootstrap_servers=BROKERS)

for message in consumer:
    msg=json.loads(message.value.decode())
   
    title_name = msg.get('title')
    like_count = msg.get('likes')
    # for v in msg:
    #     title_name=v.get('title')
    #     like_count=v.get('likes')

    #     new_data = {
    #     "title":title_name,
    #     "likes": like_count}
    print(f"{title_name} 노래는 좋아요 수가 {like_count} 으로 인기가 많다. ")
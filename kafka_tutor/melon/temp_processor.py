from kafka import KafkaConsumer,KafkaProducer
import json


TEMP_TOPIC= 'temp'
BROKERS = ["localhost:8900", "localhost:8901", "localhost:8902"]
consumer = KafkaConsumer(TEMP_TOPIC, bootstrap_servers=BROKERS)

for message in consumer:
    msg=json.loads(message.value.decode())
   
    title_name = msg.get('title')
    like_count = msg.get('likes')
 
    print(f"{title_name} 노래는 좋아요 수가 {like_count} 으로 인기가 다소 떨어진다. ")
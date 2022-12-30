from kafka import KafkaConsumer,KafkaProducer
import json
from json import loads

EXTRACT_TOPIC='melon'
TARGET_TOPIC= 'target'
TEMP_TOPIC='temp'
DECTECTOR_TOPIC='detector'

# 브로커 정보
BROKERS = ["localhost:8900", "localhost:8901", "localhost:8902"]
consumer = KafkaConsumer(EXTRACT_TOPIC, bootstrap_servers=BROKERS)
producer = KafkaProducer(bootstrap_servers =BROKERS)


# 타겟 데이터 기준 정의 함수
# 좋아요가 10만 이상인 노래 선별하기 -> 조금 더 인사이트를 얻을 만한 함수로 발전계획
def get_target_info(message):
    for va in message.values():
        like_cnt=va.get('likes')
        if int(like_cnt)> 100000:
            return True
        else:
            return False
        
  
for message in consumer:
    msg=json.loads(message.value.decode())
    temp=msg.values()
    for v in temp:
        title_name=v.get('title')
        like_count=v.get('likes')
        new_data = {
        "title":title_name,
        "likes": like_count}
        # print(new_data)

        # 조건에 맞는 경우 target_processor.py로 TARGET_TOPIC 전달
        if get_target_info(msg):
            producer.send(TARGET_TOPIC,json.dumps(new_data).encode('utf-8'))
        # 그렇지 않은 경우 temp_processor.py로 TEMP_TOPIC 전달
        else:
            producer.send(TEMP_TOPIC,json.dumps(new_data).encode('utf-8'))
        
        print(get_target_info(msg),new_data)


    
    
    
    
  




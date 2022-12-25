import datetime
import pytz # Time존 적용
import time
import random
import json
import requests
from bs4 import BeautifulSoup
from scrapy.http import TextResponse
from fake_useragent import UserAgent
import csv
from functools import reduce
import json
import requests
import pandas as pd
import pymongo
from pymongo import MongoClient


# airflow용 Mongo DB
client=pymongo.MongoClient("mongodb://localhost:27017/")
db=client.crawling
# melon=db['melon']
# melon=client.crawling.melon



# 현재 한국 시간 설정 함수
def get_seoul_date():
    utc_now = pytz.utc.localize(datetime.datetime.utcnow())
    kst_now = utc_now.astimezone(pytz.timezone("Asia/Seoul"))
    da = kst_now.strftime("%m/%d/%Y")
    ti = kst_now.strftime("%H:%M:%S")
    return da, ti

# 멜론차트에 세부내용 추출 함수
def generate_payment_data(now_date, now_time):
    melonList={}
    URL='https://www.melon.com/chart/index.htm'
    headers={
        "User-Agent": UserAgent().chrome
    }
    res=requests.get(URL,headers=headers)
    soup = BeautifulSoup(res.content,'html.parser')
    lst50=soup.select('.lst50,.lst100')
    error_keys = []
    for i in lst50:
        data={}
        idx = i.select_one('.wrap.t_right > input')["value"]
        num = i.select_one('span.rank_wrap').text.strip().split('\n')
        stage = i.select_one('span.rank_wrap').text.strip().split('\n')
        if len(num) == 1:
            num.append('0')
        if '순위 동일' in stage:
            stage=''
        elif '단계 상승' in stage:
            stage='+'
        elif '단계 하락' in stage:
            stage="-"
        elif '순위 진입' in stage:
            stage=''

        data['rank'] = i.select_one('.rank').text
        data['up_down'] = f'{stage}{num[1]}'
        data["title"] = i.select_one('.ellipsis.rank01').a.text
        data["singer"] = i.select_one('.ellipsis.rank02').a.text
        data['album'] = i.select_one('.ellipsis.rank03').a.text
        data['date'] = now_date
        data['time'] = now_time
        data["index"] = idx
       
        # 속도 측면에서 index를 활용! 
        melonList[idx] = data

    query_keys = sorted(list(melonList.keys()))
    API_URL = f"https://www.melon.com/commonlike/getSongLike.json?contsIds={','.join(map(lambda x:str(x), query_keys))}"
    headers = {
        "content-type": "application/json;charset=UTF-8",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
    }
    response = requests.get(
        API_URL,
        headers=headers
    )
    likes = response.json()['contsLike']
  
    if len(melonList) != len(likes):
        print('ERROR: melonList length not equal likes length')

    for like_info in likes:
        the_like_key = str(like_info['CONTSID'])
        the_music = melonList.get(the_like_key)
        if the_music == None:
            print(f'ERROR: cannot find the_music, {the_like_key}')
            error_keys.append(the_like_key)
            continue
        likes_cnt=(like_info["SUMMCNT"])
        the_music['likes'] = likes_cnt
    return melonList

# 최종 데이터 처리
now_date, now_time = get_seoul_date()
data = generate_payment_data(now_date, now_time)
print(data.values())
# mongo DB에 데이터 넣기
db.melon.insert_many(data.values()) 
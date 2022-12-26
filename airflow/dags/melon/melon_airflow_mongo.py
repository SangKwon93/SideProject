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


# 현재 한국 시간 설정 함수
def get_seoul_date():
    utc_now = pytz.utc.localize(datetime.datetime.utcnow())
    kst_now = utc_now.astimezone(pytz.timezone("Asia/Seoul"))
    date = kst_now.strftime("%Y.%m.%d")
    time = kst_now.strftime("%H:%M")
    return date, time


# 멜론차트에 세부내용 추출 함수
def get_melon_chart(now_date,now_time,release_date,genre,publisher,agency):
    melonList={}
    URL='https://www.melon.com/chart/index.htm'
    headers={
        "User-Agent": UserAgent().chrome
    }
    res=requests.get(URL,headers=headers)
    soup = BeautifulSoup(res.content,'html.parser')
    top100=soup.select('.lst50,.lst100')
    error_keys = []
    for i in top100:
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

        # 현재 날짜 및 시간
        data['date'] = now_date
        data['time'] = now_time
        

        # 노래 정보
        data['rank'] = i.select_one('.rank').text
        data['up_down'] = f'{stage}{num[1]}'
        data["title"] = i.select_one('.ellipsis.rank01').a.text
        data["singer"] = i.select_one('.ellipsis.rank02').a.text
        data['album'] = i.select_one('.ellipsis.rank03').a.text
        
        
        # 앨범 세부 내용
        data['album_date']=release_date
        data['album_genre']=genre
        data['album_publisher']=publisher
        data['alum_agency']=agency
        
        # 노래 고유번호 인덱스 활용
        data["index"] = idx
       
        # 속도 측면에서 index를 활용! 
        melonList[idx] = data

    query_keys = sorted(list(melonList.keys()))
    API_URL = f"https://www.melon.com/commonlike/getSongLike.json?contsIds={','.join(map(lambda x:str(x), query_keys))}"
    headers = {
        "content-type": "application/json;charset=UTF-8",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
    }
    response = requests.get(API_URL, headers=headers)
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

def get_album_info():
    
    # 멜론 TOP100 차트 사이트 접속
    URL='https://www.melon.com/chart/index.htm'
    
    headers = {
        "content-type": "application/json;charset=UTF-8",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
    }

    res=requests.get(URL,headers=headers)
    soup = BeautifulSoup(res.content,'html.parser')
    response=TextResponse(res.url,body=res.text, encoding='utf-8')
    top100=soup.select('.lst50,.lst100')

    
    
    for i in top100:
        album_code_lst=[]
        album_code = i.select_one('.wrap_song_info > div.ellipsis.rank03> a')["href"].split("'")[1]
       
       # 세부 페이지 접속
        ALBUM_URL=f'https://www.melon.com/album/detail.htm?albumId={album_code}'
        
    
        res=requests.get(ALBUM_URL,headers=headers)
        response=TextResponse(res.url,body=res.text,encoding='utf-8')
        # 날짜
        release_date =response.xpath('//*[@id="conts"]/div[2]/div/div[2]/div[2]/dl/dd[1]/text()').extract_first()
        # 장르
        genre =response.xpath('//*[@id="conts"]/div[2]/div/div[2]/div[2]/dl/dd[2]/text()').extract_first()
        # 발매사
        publisher=response.xpath('//*[@id="conts"]/div[2]/div/div[2]/div[2]/dl/dd[3]/text()').extract_first()
        # 기획사
        agency=response.xpath('//*[@id="conts"]/div[2]/div/div[2]/div[2]/dl/dd[4]/text()').extract_first()

    return release_date,genre,publisher,agency

        
# 최종 데이터 처리
now_date, now_time = get_seoul_date()
release_date,genre,publisher,agency = get_album_info()
data = get_melon_chart(now_date, now_time, release_date,genre,publisher,agency)
print(data.values())

#mongo DB에 데이터 넣기
db.melon.insert_many(data.values()) 
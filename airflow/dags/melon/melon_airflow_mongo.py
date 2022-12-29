# 최종 함수화 완성
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


# Linux에서 활용 시 한국 시간 설정 함수
def get_seoul_date():
    utc_now = pytz.utc.localize(datetime.datetime.utcnow())
    kst_now = utc_now.astimezone(pytz.timezone("Asia/Seoul"))
    date = kst_now.strftime("%m/%d/%Y")
    time = kst_now.strftime("%H:%M")
    return date, time


# 멜론 차트 데이터 추출
def get_melon_chart(now_date,now_time):
    melonList={}
    URL='https://www.melon.com/chart/index.htm'
    headers={
        "User-Agent": UserAgent().chrome
    }
    res=requests.get(URL,headers=headers)
    response=TextResponse(res.url,body=res.text,encoding='utf-8')
    soup = BeautifulSoup(res.content,'html.parser')
    top100=soup.select('.lst50,.lst100')
    error_keys = []

    for i in top100:
        data={}
        idx = str(i.select_one('.wrap.t_right > input')["value"])
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

        # # 현재 날짜 및 시간
        data['date'] = now_date
        data['time'] = now_time
        
        # 노래 정보
        data['rank'] = i.select_one('.rank').text
        data['up_down'] = f'{stage}{num[1]}'
        data["title"] = i.select_one('.ellipsis.rank01').a.text
        data["singer"] = i.select_one('.ellipsis.rank02').a.text
        data['album'] = i.select_one('.ellipsis.rank03').a.text
        data['album_code']=i.select_one('.wrap_song_info > div.ellipsis.rank03> a')["href"].split("'")[1]
        
        # 노래 고유번호 인덱스 활용
        data["index"] = idx
       
        # 속도 측면에서 index를 활용! 
        melonList[idx] = data

    return melonList
    

# 멜론 좋아요 데이터 추출 - fetch/XHR에서 가져오기
def get_likes(music_list):
    
    # 딕셔너리 형태로 리턴받기 위해 빈 딕셔너리 작성
    ret = {}
   
    # 각 노래별 고유번호 data-song-no 가져오기(index 값으로 key로 활용)
    query_keys = sorted(list(music_list.keys()))
    
    # 실시간 가져오는 링크 작성
    API_URL = f"https://www.melon.com/commonlike/getSongLike.json?contsIds={','.join(map(lambda x:str(x), query_keys))}"
    headers = {
        "content-type": "application/json;charset=UTF-8",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
    }
    response = requests.get(API_URL, headers=headers)

    # 좋아요 수 데이테만 확보
    likes = response.json()['contsLike']
  
    # 멜론 차트 100개 데이터와 좋아요 100개 데이터가 맞는지 확인
    if len(music_list) != len(likes):
        print('ERROR: melonList length not equal likes length')

    # CONTSID를 활용하여 좋아요 수와 노래별 고유번호를 매칭 후 딕셔너리 형태로 변환
    for like_info in likes:
        the_key = str(like_info['CONTSID'])
        the_likes_cnt=(like_info["SUMMCNT"])
        ret[the_key] = the_likes_cnt
    return ret

# 앨범 세부 내역 추출
def get_albums(musics):
    
    ret={}
    
    # Network에서 User 정보 가져오기
    headers = {
        "content-type": "application/json;charset=UTF-8",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
    }
    #딕셔너리 형태의 values 값 중에서
    for music in musics.values():
        album_data = {}
        
        # 원하고자 하는 값 가져오기
        the_key = music.get('index')
        album_code = music.get('album_code')

        # 각 URL 에서 데이터 수집
        ALBUM_URL=f'https://www.melon.com/album/detail.htm?albumId={album_code}'
        res=requests.get(ALBUM_URL,headers=headers)
        response=TextResponse(res.url,body=res.text,encoding='utf-8')

        album_data['release_date']=response.xpath('//*[@id="conts"]/div[2]/div/div[2]/div[2]/dl/dd[1]/text()').extract_first()
        album_data['genre']=response.xpath('//*[@id="conts"]/div[2]/div/div[2]/div[2]/dl/dd[2]/text()').extract_first()
        album_data['publisher']=response.xpath('//*[@id="conts"]/div[2]/div/div[2]/div[2]/dl/dd[3]/text()').extract_first()
        album_data['agency']=response.xpath('//*[@id="conts"]/div[2]/div/div[2]/div[2]/dl/dd[4]/text()').extract_first()

        ret[the_key] = album_data
    return ret


# 함수마다 딕셔너리 형태의 데이터 합쳐주기
def merge_music(musics, likes={}, albums={}): #{} 매개변수 기본값
    
    ret = {} 
 
    for music in musics.values():
        the_key = music['index']
       
        # detail_music['likes'] = likes.get(the_key)
        # detail_music = {**detail_music, **albums.get(the_key)}
        # {**music} == { title: music.title, singer: music.singer...}
        # {**album} == {genre, title, }
        # {**music, **album} = {title, ,dke,dke,ld,dekled,.dekl}
        # album = albums.get(the_key) #{genre, title, code}
        ret[the_key] = {**music, **albums.get(the_key), 'likes': likes.get(the_key)} # ret.append(detail_music)
    return ret

# 메인 함수 - 작성 함수 활용하여 데이터 값 처리
def main():
    now_date, now_time = get_seoul_date()
    musics = get_melon_chart(now_date,now_time) # { 1: {name, title, albumId, }}
    likes = get_likes(musics) # { 1 : 4311, 2: 321}
    albums = get_albums(musics) # { 1 : {}, 2: {}, 3: {}
    detail_musics = merge_music(musics, likes, albums)

    return detail_musics

# 출력
#print(main().values())

#mongo DB에 데이터 넣기
db.melon.insert_many(main().values()) 

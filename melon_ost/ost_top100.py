import re
import time
import pymongo

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

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
import random
import requests

# 브라우저 열기
def get_chrome_driver():
    options = webdriver.ChromeOptions()
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    driver = webdriver.Chrome('chromedriver.exe', options=options)
    return driver

driver= get_chrome_driver()

def get_melon_chart():
    musics={}
    URL='https://www.melon.com/chart/month/index.htm?classCd=GN1500'
    
    headers={
        "User-Agent": UserAgent().chrome
    }
    driver.get(URL)
    driver.implicitly_wait(10)
    res=requests.get(URL,headers=headers)
    soup = BeautifulSoup(res.content,'html.parser')
    
    top100=soup.select('.lst50,.lst100')

    # 월별로 도는 로직(12달)
    for num in range(1,13):
        this_month_musics = {}
        URL='https://www.melon.com/chart/month/index.htm?classCd=GN1500'
        driver.get(URL)
        driver.implicitly_wait(10)
        driver.find_element(By.XPATH,'//*[@id="conts"]/div[3]/div/button').click() # 화살표 클릭
        num_xpath=f'//*[@id="conts"]/div[3]/div/div/dl/dd[1]/ul/li[{num}]/a' # 1~12월 클릭
        driver.find_element(By.XPATH,num_xpath).click() # 월 클릭
        month_num=f'//*[@id="conts"]/div[3]/div/div/dl/dd[1]/ul/li[{num}]/a'
        month = driver.find_element(By.XPATH,month_num).get_attribute('data-date')
        driver.find_element(By.XPATH,'//*[@id="conts"]/div[3]').click() # 빈공간
        day = driver.find_element(By.XPATH,'//*[@id="conts"]/div[3]/span').text
        
        # URL2 는 Network - Fetch/XHR에서 월별로 URL2 주소가 변경됨을 확인함
        # (기존 URL은 변동이 없고 URL2가 변경) 
        URL2=f'https://www.melon.com/chart/month/index.htm?classCd=GN1500&moved=Y&rankMonth={month}'
        headers={"User-Agent": UserAgent().chrome}
        driver.get(URL2)
        driver.implicitly_wait(10)
        driver.maximize_window()
        res=requests.get(URL2,headers=headers)
        soup = BeautifulSoup(res.content,'html.parser')
        top100=soup.select('.lst50,.lst100')

        # 곡별로 도는 로직(100곡)
        for i in top100:
            music = {}
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
            music['rank'] = i.select_one('.rank').text
            music['day'] = day
            music['up_down'] = f'{stage}{num[1]}'
            music["title"] = i.select_one('.ellipsis.rank01').a.text
            music["singer"] = i.select_one('.ellipsis.rank02').a.text
            music['album'] = i.select_one('.ellipsis.rank03').a.text
            music['album_code']=i.select_one('.wrap_song_info > div.ellipsis.rank03> a')["href"].split("'")[1]
            music['ost_date']=month
            music["index"] = idx
            this_month_musics[idx] = music
        
        # 달별로 한번 더 묶기
        # 한번 더 딕셔너리 형태로 묶은 이유는 인덱스로 묶게되면 12달간 겹치는 곡들을 하나의 값으로 잘못 인식
        musics[month] = this_month_musics
    return musics

output = get_melon_chart()

# 시각화를 위해 csv파일 저장
df= pd.DataFrame(output)
df.to_csv("ost_melon_top100_2022.csv")

'''
해결하기 어려웠던 점
Q : 자꾸 for문 안에서 돌아가며 데이터를 받아왔는데 딕셔너리에 하나의 값만 들어온다.
A : 빈 딕셔너리 변수를 for문 안에서 돌리면 계속 초기화되며 돌아간다. 

Q : 하나의 값만 반환된다.
A : return이 진행되면 종료된다. for문 돌릴 때 들여쓰기 주의할것! 

'''

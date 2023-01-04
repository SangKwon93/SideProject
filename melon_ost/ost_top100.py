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
ua=UserAgent()

def get_chrome_driver():
    chrome_options = webdriver.ChromeOptions()
    # chrome_options.headless = True

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )
    
    return driver

driver= get_chrome_driver()


URL='https://www.melon.com/chart/month/index.htm?classCd=GN1500'
headers={
        "user-agent": "ua.random"
    }
driver.get(URL)
driver.implicitly_wait(10)   
res=requests.get(URL,headers=headers)
response=TextResponse(res.url,body=res.text,encoding='utf-8')
soup = BeautifulSoup(res.content,'html.parser')


# 화살표 클릭
driver.find_element(By.XPATH,'//*[@id="conts"]/div[3]/div/button').click()

# 월 클릭
driver.find_element(By.XPATH,'//*[@id="conts"]/div[3]/div/div/dl/dd[1]/ul/li[1]/a').click()

# 빈공간 클릭
driver.find_element(By.XPATH,'//*[@id="conts"]/div[3]').click()

def get_melon_chart():
    melonList={}

    top100=soup.select('.lst50,.lst100')

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
    print(melonList)

# 화살표 클릭
driver.find_element(By.XPATH,'//*[@id="conts"]/div[3]/div/button').click()

# 1~12월 데이터 수집

for num in range(1,13):
    # 화살표 클릭
    driver.find_element(By.XPATH,'//*[@id="conts"]/div[3]/div/button').click()
    
    # 1~12월 클릭
    num_xpath=f'//*[@id="conts"]/div[3]/div/div/dl/dd[1]/ul/li[{num}]/a'
    driver.find_element(By.XPATH,num_xpath).click()

    # 빈공간 클릭
    driver.find_element(By.XPATH,'//*[@id="conts"]/div[3]').click()
    print(get_melon_chart())
    
# 수정본 부탁본
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

def get_chrome_driver():
    options = webdriver.ChromeOptions()
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    driver = webdriver.Chrome('chromedriver.exe', options=options)
    return driver

# def get_chrome_driver():
#     chrome_options = webdriver.ChromeOptions()
#     # chrome_options.headless = True

#     driver = webdriver.Chrome(
#         service=Service(ChromeDriverManager().install()),
#         options=chrome_options
#     )
    
#     return driver
driver= get_chrome_driver()

def ost_chart_date():
    URL='https://www.melon.com/chart/month/index.htm?classCd=GN1500'
    headers={"User-Agent": UserAgent().chrome}
    res=requests.get(URL,headers=headers)
    soup = BeautifulSoup(res.content,'html.parser')
    date=soup.select_one('#conts > div.calendar_prid > span').text.strip()
    return date

def get_melon_chart():
    melonList={}
    URL='https://www.melon.com/chart/month/index.htm?classCd=GN1500'
    
    headers={
        "User-Agent": UserAgent().chrome
    }
    driver.get(URL)
    driver.implicitly_wait(10)
    res=requests.get(URL,headers=headers)
    soup = BeautifulSoup(res.content,'html.parser')
    top100=soup.select('.lst50,.lst100')
    # # 화살표 클릭
    # driver.find_element(By.XPATH,'//*[@id="conts"]/div[3]/div/button').click() 
    # # 1월 클릭
    # driver.find_element(By.XPATH,'//*[@id="conts"]/div[3]/div/div/dl/dd[1]/ul/li[1]/a').click()
    # # 빈공간 클릭
    # driver.find_element(By.XPATH,'//*[@id="conts"]/div[3]').click()
    # month=driver.find_element(By.XPATH,f'#conts > div.calendar_prid > div > div > dl > dd.month_calendar > ul > li:nth-child({number}) > a > span')
    
    for num in range(1,5):
        data={}
        # URL='https://www.melon.com/chart/month/index.htm?classCd=GN1500'
        # driver.get(URL)
        # driver.implicitly_wait(10) 
        # 화살표 클릭
        driver.find_element(By.XPATH,'//*[@id="conts"]/div[3]/div/button').click()
        
        # 1~12월 클릭
        num_xpath=f'//*[@id="conts"]/div[3]/div/div/dl/dd[1]/ul/li[{num}]/a'
        driver.find_element(By.XPATH,num_xpath).click()
        # 월 클릭
        
        month_num=f'//*[@id="conts"]/div[3]/div/div/dl/dd[1]/ul/li[{num}]/a'
        month = driver.find_element(By.XPATH,month_num).get_attribute('data-date')
        #data['ost_date']=month
        # 빈공간
        driver.find_element(By.XPATH,'//*[@id="conts"]/div[3]').click() 
        
        
        #month=soup.select_one('#conts > div.calendar_prid > span').text.strip()
        # month=soup.select_one('#conts > div.calendar_prid > span').text.strip()
        # data['ost_date']=month
        # res=requests.get(URL,headers=headers)
        # soup = BeautifulSoup(res.content,'html.parser')
        # top100=soup.select('.lst50,.lst100')
        # month=soup.select_one('#conts > div.calendar_prid > span').text.strip()
        # data={}
        # data['chart_date']=soup.select_one('#conts > div.calendar_prid > span').text.strip()
        # month=soup.select_one('#conts > div.calendar_prid > span').text.strip()
        # data['ost_date']=month
        stop=False
        while True:
            for i in top100:
                data={}
                
                # month=soup.select_one('#conts > div.calendar_prid > span').text.strip()
                data['ost_date']=month
                # print(data)
                # data['chart_date']=month
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
                
            # data={}
            # for num in range(1,13):
            #     data={}
            #     # URL='https://www.melon.com/chart/month/index.htm?classCd=GN1500'
            #     # driver.get(URL)
            #     # driver.implicitly_wait(10) 
            #     # 화살표 클릭
            #     driver.find_element(By.XPATH,'//*[@id="conts"]/div[3]/div/button').click()
                
            #     # 1~12월 클릭
            #     num_xpath=f'//*[@id="conts"]/div[3]/div/div/dl/dd[1]/ul/li[{num}]/a'
            #     driver.find_element(By.XPATH,num_xpath).click()

                # 속도 측면에서 index를 활용! 
                melonList[idx] = data
            return melonList
        # print(melonList)
            # chart_date=ost_chart_date()
print(get_melon_chart())
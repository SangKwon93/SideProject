import scrapy
import json
import requests
from scrapy.http import TextResponse
from bs4 import BeautifulSoup
import os
import threading
from multiprocessing import Process
import time
from fake_useragent import UserAgent

headers = {"User-Agent" : UserAgent().chrome }
#res = requests.get(url,headers=headers)

from MaeilNews.items import MaeilnewsItem


class Spider(scrapy.Spider):
    name="MaeilNews"
    allow_dimain=["www.mk.co.kr"]
    
    custom_settings = {
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'scrapy_fake_useragent.middleware.RandomUserAgentMiddleware': 400,
        }
    }
    
    # def get_link(self, add):
    #     link_list=[]
    #     url=f"https://www.mk.co.kr/news/economy/?page={add}"
    #     req= requests.get(url)
    #     #soup=BeautifulSoup(req.text,'html.parser')
    #     soup1=BeautifulSoup(req.content.decode('euc-kr', 'replace'), 'html.parser')
    #     response = TextResponse(req.url, body=req.text, encoding='utf-8')
    #     tt=soup1.find('div',class_='list_area')
    #     tt1 = tt.find_all('dt',class_='tit')
    
    #     for dt in tt1:
    #         link_list.append(dt.select_one('a')['href'])
    #     return link_list
        
    
    def start_requests(self):
        # url='https://www.mk.co.kr/news/economy/'
        # headers= {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'}
        url='https://www.mk.co.kr/news/economy/?page={}'
        res= requests.get(url,headers=headers)
        
       
    
        
        for page in range(1):
            yield scrapy.Request(url.format(page),callback=self.parse)
        
        
    def parse(self, response):
        url_list = response.xpath('//*[@class="list_area"]/dl/dt/a/@href').extract()
        
        for link in url_list:
            link = link.replace('m.mk', 'www.mk')
            #yield scrapy.Request(link, callback=self.detail_info)
            yield scrapy.Request(link,callback=self.detail_info)
        
#meta={'dont_redirect': True,'handle_httpstatus_list': [302]},

    def detail_info(self,response):
        item = MaeilnewsItem()
        soup = BeautifulSoup(response.text)
        
        if response.url[8] == 'w':
            # 웹페이지
            item["article_title"]= response.xpath('//*[@id="top_header"]/div/div/h1/text()').extract_first()
            item["article_time"]= response.xpath('//*[@id="top_header"]/div/div/div[1]/ul/li[2]/text()').extract_first() 
            #item["article_text"]= soup.select_one('#article_body').text 
            item["article_cotent"]= response.xpath('//*[@id="article_body"]/div/text()').extract()
        else :
        # elif response.url[8] == 'm':
            # 모바일페이지
            item["article_title"]= soup.select_one('div.head_tit').text #모바일
            item["article_time"] = soup.select_one('div.source_date > div:nth-child(1) > span').text
            #item["article_text"]= soup.select_one('#artText').text #모바일
            item["article_cotent"] = response.xpath('//*[@id="artText"]/text()').extract()
    
        yield item
        
        
        
      
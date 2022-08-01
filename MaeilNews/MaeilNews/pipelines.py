# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from matplotlib import collections
import pymongo



class MaeilnewsPipeline:
    def process_item(self, item, spider):
         
        client = pymongo.MongoClient('mongodb://team04:1111@ec2-54-95-8-243.ap-northeast-1.compute.amazonaws.com:27017/')
        # user = 'team04'
        # pw = '1111'
        # host = 'ec2-54-95-8-243.ap-northeast-1.compute.amazonaws.com'
        # client = pymongo.MongoClient(f'mongodb://{user}:{pw}@{host}:27017/')
        db = client.resell
        
        
        #"title": item["article_title"]
        data = { "title": item["article_title"], 
                 "time": item["article_time"],
                 "content": item["article_cotent"], 
               }
        
        db.news.insert_one(data)
    
        return item
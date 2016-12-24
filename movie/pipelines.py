# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html


#es = Elasticsearch(hosts=[('localhost', 9200)])

from elasticsearch import Elasticsearch
from elasticsearch_dsl.mapping import Mapping



class MoviePipeline(object):
    def process_item(self, item, spider):
        return item

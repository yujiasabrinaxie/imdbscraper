# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from elasticsearch import Elasticsearch
from elasticsearch_dsl.document import DocType
from elasticsearch_dsl.mapping import Mapping
from elasticsearch_dsl.field import String, Integer, Date

es = Elasticsearch()

class Movie(DocType):
    title = String()
    vote = Integer()
    creators = String(multi=True)

    class Meta:
        index = 'imdb'

Movie.delete(using=es)
Movie.init(using=es)

class MoviePipeline(object):
    def process_item(self, item, spider):
        movie = Movie(title=item['title'], vote=item['vote'])
        movie.creators = item['creators']
        movie.save(using=es)
        return item

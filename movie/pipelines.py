# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Index
from elasticsearch_dsl.document import DocType
from elasticsearch_dsl.field import Integer, Date, Float, Keyword, Text

es = Elasticsearch()


class Movie(DocType):
    title = Text(fields={'raw': {'type': 'keyword'}})
    vote = Integer()
    creators = Keyword(multi=True)
    genre = Keyword(multi=True)
    cast = Keyword(multi=True)
    time = Integer()
    country = Keyword(multi=True)
    plot_keywords = Keyword(multi=True)
    language = Keyword(multi=True)
    filming_locations = Keyword(multi=True)
    release_date = Date()
    ratingvalue = Float()
    poster = Keyword()
    summary = Text()

    class Meta:
        index = 'imdb'



class MoviePipeline(object):
    def __init__(self):
        movies = Index('imdb', using=es)
        movies.doc_type(Movie)
        movies.delete(ignore=404)
        movies.create()

    def process_item(self, item, spider):
        movie = Movie(title=item['title'], vote=item['vote'])
        movie.creators = item['creators']
        movie.genre = item['genre']
        movie.cast = item['cast']
        movie.time = item['time']
        movie.country = item['country']
        movie.plot_keywords = item['plot_keywords']
        movie.language = item['language']
        movie.filming_locations = item['filming_locations']
        movie.release_date = item['release_date']
        movie.ratingvalue = item['ratingvalue']
        movie.poster = item['poster']
        movie.summary = item['summary']
        movie.save(using=es)
        return item


import scrapy
import json
import time
import re


class moviespider(scrapy.Spider):
    name = "imdb_spider"
    start_urls = [
        """http://www.imdb.com/search/title?release_date=2010-01-01,2016-12-31&user_rating=8.0,10"""
    ]
    imdbhome = 'http://www.imdb.com'
    cur_page = 1
    handle_httpstatus_list = [400, 401, 403, 404, 405, 413, 500]

    def parse(self, response):
        movie_hrefs = map(lambda x: 'http://www.imdb.com/' + x,
                          response.xpath('//h3[@class="lister-item-header"]/a/@href').extract())
        total_matching = filter(lambda x: 'titles' in x, response.xpath('//div[@class="desc"]/text()').extract())[0]
        begin = begin = re.search(' \d', total_matching)
        end = re.search('\d ', total_matching)
        total_matching = int(filter(lambda x: x.isdigit(), total_matching[begin.start() + 1: end.start() + 1]))
        numpages = total_matching / 50
        while self.cur_page < numpages:
            for href in movie_hrefs:
                yield scrapy.Request(href, callback=self.parseMovie)
            self.cur_page += 1
            yield scrapy.Request(self.get_page_url(self.cur_page), callback=self.parse)

    def parseMovie(self, response):
        if response.status == 301:
            yield scrapy.Request(self.imdbhome, callback=self.parse)
        title = response.xpath("//h1[@itemprop='name']/text()").extract()
        vote = response.xpath("//span[@itemprop='ratingCount']/text()").extract()
        genre = response.xpath("//span[@itemprop='genre']/text()").extract()
        #creators =response.xpath("//h4[@class='inline']span/a/span/[@itemprop='name']/text()").extract()

        #first_two_cast = response.xpath("//span[@itemprop='name'][2]/text").extract()
        #time = response.xpath("//time[@datetime]/text").extract()


        yield dict(genre=genre,vote=vote, title=title)


    def get_page_url(self, page):
        return
        """http://www.imdb.com/search/title?release_date=2010-01-01,2016-12-31&user_rating=8.0,10&page={}""".format(page)

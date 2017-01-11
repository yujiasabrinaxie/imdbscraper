
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
        title = response.xpath("//h1[@itemprop='name']/text()").extract_first()
        vote = response.xpath("//span[@itemprop='ratingCount']/text()").extract()[-1]
        genre = response.xpath("//span[@itemprop='genre']/text()").extract()
        creators =response.xpath("//span[@itemprop='creator']//span[@itemprop='name']/text()").extract()
        cast = response.xpath("//td[@itemprop='actor']//span[@itemprop='name']/text()").extract()
        #first_two_cast = response.xpath("//td[@itemprop='actor']//span[@itemprop='name']/text()").extract()[:2]
        time = response.xpath("//time[@datetime]/text()").extract()[-1]
        plot_keywords = response.xpath("//div[@itemprop='keywords']//span[@itemprop='keywords']/text").extract()
        country = response.xpath("//h4[@class='inline']//a[@itemprop='url']/text()").extract()
        language = response.xpath("h4[contains(text(),'Language')]/..text()").extract()
        filming_locations = response.xpath("h4[contains(text(),'Filming Locations')]/..text()").extract()
        release_date = response.xpath("h4[contains(text(),'Release Date:')]/../text()").extract()

        yield {'genre': self.normalize_list(genre),
               'vote': self.normalize_int(self.normalize_string(vote)),
               'title': self.normalize_string(title),
               'creators':self.normalize_list(creators),
               'cast':self.normalize_list(cast),
               'time': self.normalize_int(self.normalize_string(time)),
               'country': self.normalize_string(country),
               'plot_keywords': self.normalize_string(plot_keywords),
               'language': self.normalize_string(language),
               'filming_locations': self.normalize_list(filming_locations),
               'release_date': self.normalize_string(release_date),
               }


    def get_page_url(self, page):
        return "http://www.imdb.com/search/title?release_date=2010-01-01,2016-12-31&user_rating=8.0,10&page=%d" % page

    def normalize_string(self, s):
        return str(filter(lambda x: ord(x) < 128, s)).rstrip()

    def normalize_list(self, l):
        return [self.normalize_string(x) for x in l]

    def normalize_int(self, s):
        return int(filter(lambda x: x.isdigit(), s))
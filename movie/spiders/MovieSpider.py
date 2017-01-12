import scrapy
import re
import logging
import sys
import time

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

month_mapping = {'January': '01',
                 'February': '02',
                 'March': '03',
                 'April': '04',
                 'May': '05',
                 'June': '06',
                 'July': '07',
                 'August': '08',
                 'September': '09',
                 'October': '10',
                 'November': '11',
                 'December': '12'}


class moviespider(scrapy.Spider):
    name = "imdb_spider"
    start_urls = [
        """http://www.imdb.com/search/title?release_date=1980-01-01,2018-01-01&title_type=feature&user_rating=7.0,10"""
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
        logger.debug('totally %d pages and current scraping page %d', numpages, self.cur_page)

        logger.debug('Number of movies page %d is %d', self.cur_page, len(movie_hrefs))
        for href in movie_hrefs:
            yield scrapy.Request(href, callback=self.parseMovie)
            time.sleep(1)
        self.cur_page += 1
        if self.cur_page < numpages:
            yield scrapy.Request(self.get_page_url(self.cur_page), callback=self.parse)

    def parseMovie(self, response):
        if response.status == 301:
            yield scrapy.Request(self.imdbhome, callback=self.parse)
        title = response.xpath("//h1[@itemprop='name']/text()").extract_first()
        vote = response.xpath("//span[@itemprop='ratingCount']/text()").extract()[-1]
        genre = response.xpath("//span[@itemprop='genre']/text()").extract()
        creators = response.xpath("//span[@itemprop='creator']//span[@itemprop='name']/text()").extract()
        cast = response.xpath("//td[@itemprop='actor']//span[@itemprop='name']/text()").extract()
        time = response.xpath("//time[@datetime]/text()").extract()[-1]
        plot_keywords = response.xpath("//div[@itemprop='keywords']//span[@itemprop='keywords']/text()").extract()
        ratingvalue = response.xpath("//div[@class='ratingValue']//span[@itemprop='ratingValue']/text()").extract()[0]
        release_date = response.xpath("//h4[text()='Release Date:']/../text()").extract()[1]
        country = response.xpath("//div[h4[text() = 'Country:']]/a/text()").extract()
        language = response.xpath("//div[h4[text() = 'Language:']]/a/text()").extract()
        filming_locations = response.xpath("//div[h4[text() = 'Filming Locations:']]/a/text()").extract()
        poster = response.xpath('//div[@class="poster"]//img/@src').extract()[0]
        summary = response.xpath('//div[@class="summary_text"]/text()').extract()[0]

        yield {'genre': self.normalize_list(genre),
               'vote': self.normalize_int(self.normalize_string(vote)),
               'title': self.normalize_string(title),
               'creators': self.normalize_list(creators),
               'cast': self.normalize_list(cast),
               'time': self.normalize_int(self.normalize_string(time)),
               'country': self.normalize_list(country),
               'plot_keywords': self.normalize_list(plot_keywords),
               'language': self.normalize_list(language),
               'filming_locations': self.normalize_list(filming_locations),
               'release_date': self.normalize_date(release_date),
               'ratingvalue': self.normalize_float(ratingvalue),
               'poster': poster,
               'summary': self.normalize_string(summary)
               }

    def get_page_url(self, page):
            return """http://www.imdb.com/search/title?release_date=1980-01-01,2018-01-01&title_type=feature&user_rating=7.0,10&page=%d""" % page

    def normalize_string(self, s):
        return str(filter(lambda x: ord(x) < 128, s)).rstrip().lstrip()

    def normalize_list(self, l):
        return [self.normalize_string(x) for x in l]

    def normalize_int(self, s):
        return int(filter(lambda x: x.isdigit(), s))

    def normalize_date(self, s):
        date = self.normalize_string(s)
        splited_date = date.split(' ')
        splited_date[1] = month_mapping[splited_date[1]]
        if int(splited_date[0]) < 10:
            splited_date[0] = '0' + splited_date[0]
        print 'splited date is %s' % splited_date
        print 'reversed splited date is %s' % splited_date[:3][::-1]
        return '-'.join(splited_date[:3][::-1])  # reverse

    def normalize_float(self, s):
        return float(filter(lambda x: x.isdigit() or x == '.', s))

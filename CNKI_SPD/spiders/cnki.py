# -*- coding: utf-8 -*-
import scrapy


# import scrapy_redis

class CnkiSpider(scrapy.Spider):
    name = 'cnki'
    allowed_domains = ['cnki.net']
    # 电化教育研究	1003-1553  -> DHJY
    start_urls = ['pcode://DHJY']
    pcode = ''
    pykm = ''
    year = ''
    Issue = ''
    Entry = ''
    journal_base_url = f'http://navi.cnki.net/knavi/JournalDetail?pcode={pcode}&pykm={pykm}&Year={year}&Issue={Issue}&Entry={Entry}'

    def parse_journal_detail(self, response):
        # get pramas
        pass

    def parse_paper(self, response):
        pass

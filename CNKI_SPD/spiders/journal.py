# -*- coding: utf-8 -*-
import scrapy
from scrapy import Request, Spider
import re
from scrapy.selector import Selector, SelectorList
import urllib
from pymongo import MongoClient
import copy
from CNKI_SPD.utils import get_md5
from urllib.parse import urlsplit, parse_qs, urljoin, urlunparse
from math import ceil
import time
from datetime import datetime
import json

uid = 'uid=WEEvREdxOWJmbC9oM1NjYkZCcDMwV2RyYm5Xb3ZCekFhUDlnQUxxL0ZSUTI%3D%24R1yZ0H6jyaa0en3RxVUd8df-oHi7XMMDo7mtKT6mSmEvTuk11l2gFA!!'
sample_headers = {
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Language': 'zh,zh-CN;q=0.9,en;q=0.8',
}


# todo 那个网格检索的接口解析掉，东西全在下面的链接，和ref差不多的方式
class JournalSpider(Spider):
    name = 'journal'
    allowed_domains = ['cnki.net']
    base_url = 'kns.cnki.net'
    detail_url = 'kns.cnki.net/kcms/detail/detail.aspx?dbcode={dbcode}&filename={filename}&dbname={dbname}'
    # 电化教育研究	1003-1553  -> DHJY
    start_urls = ['DHJY', 'ZDJY', 'YUAN', 'DDJY', 'JFJJ', 'YCJY', 'XJJS', 'XDYC']
    # for debug
    # start_urls=['DHJY']
    pcode = 'CJFD'
    start_year = 2018
    end_year = 2019
    start_issue = 1
    end_issue = 2
    custom_settings = {
        'DEFAULT_REQUEST_HEADERS': sample_headers
    }

    def start_requests(self):
        pcode = self.pcode
        pageIdx = 0
        for pykm in self.start_urls:
            for year in range(self.start_year, self.end_year):
                for _issue in range(self.start_issue, self.end_issue):
                    issue = str(_issue).rjust(2, '0')
                    journal_url = f'http://navi.cnki.net/knavi/JournalDetail?year={year}&issue={issue}&pykm={pykm}&pageIdx={pageIdx}&pcode={pcode}'
                    yield Request(headers=sample_headers,
                                  url=journal_url,
                                  callback=self.parse_journal_info,
                                  cb_kwargs={'pykm': pykm}
                                  )

    def parse_journal_info(self, response, pykm=''):
        selector = Selector(response)
        # parse journal_info
        title_1 = selector.xpath('//h3[contains(@class,"titbox")]/text()').get()
        title_2 = selector.xpath('//h3[contains(@class,"titbox")]/p/text()').get()
        journalType = selector.xpath('//p[@class="journalType"]//text()').getall()
        if not journalType:
            journalType = ''.join(journalType)
        core_journal = 'yes' if '核心期刊' in journalType else 'no'
        CSSCI = 'yes' if 'CSSCI' in journalType else 'no'
        sponser = selector.xpath('//ul[@id="JournalBaseInfo"]//p[starts-with(text(),"主办单位")]/span/text()').get()
        pub_periodicity = selector.xpath('//ul[@id="JournalBaseInfo"]//p[starts-with(text(),"出版周期")]/span/text()').get()
        ISSN = selector.xpath('//ul[@id="JournalBaseInfo"]//p[starts-with(text(),"ISSN")]/span/text()').get()
        CN = selector.xpath('//ul[@id="JournalBaseInfo"]//p[starts-with(text(),"CN")]/span/text()').get()
        pub_place = selector.xpath('//ul[@id="JournalBaseInfo"]//p[starts-with(text(),"出版地")]/span/text()').get()
        language = selector.xpath('//ul[@id="JournalBaseInfo"]//p[starts-with(text(),"语种")]/span/text()').get()
        book_size = selector.xpath('//ul[@id="JournalBaseInfo"]//p[starts-with(text(),"开本")]/span/text()').get()
        post_release_code = selector.xpath(
            '//ul[@id="JournalBaseInfo"]//p[starts-with(text(),"邮发代号")]/span/text()').get()
        start_year_of_publication = selector.xpath(
            '//ul[@id="JournalBaseInfo"]//p[starts-with(text(),"创刊时间")]/span/text()').get()
        series_name = selector.xpath('//ul[@id="publishInfo"]//p[starts-with(text(),"专辑名称")]/span/text()').get()
        subject_name = selector.xpath('//ul[@id="publishInfo"]//p[starts-with(text(),"专题名称")]/span/text()').get()
        the_number_of_published_articles = selector.xpath(
            '//ul[@id="publishInfo"]//p[starts-with(text(),"出版文献量")]/span/text()').get()
        downloads = selector.xpath('//ul[@id="publishInfo"]//p[starts-with(text(),"总下载次数")]/span/text()').get()
        cites = selector.xpath('//ul[@id="publishInfo"]//p[starts-with(text(),"总被引次数")]/span/text()').get()
        item = {}
        item['title_1'] = title_1
        item['title_2'] = title_2
        item['pykm'] = pykm
        item['core_journal'] = core_journal
        item['CSSCI'] = CSSCI
        item['sponsor'] = sponser
        item['pub_periodicity'] = pub_periodicity
        item['ISSN'] = ISSN
        item['CN'] = CN
        item['pub_place'] = pub_place
        item['language'] = language
        item['book_size'] = book_size
        item['post_release_code'] = post_release_code
        item['start_year_of_publication'] = start_year_of_publication
        item['series_name'] = series_name
        item['subject_name'] = subject_name
        item['the_number_of_published_articles'] = the_number_of_published_articles
        item['downloads'] = downloads
        item['cites'] = cites
        item['whichtable'] = 'Journal_Info'
        self.check_item(item)
        yield item

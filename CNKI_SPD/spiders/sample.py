# -*- coding: utf-8 -*-
import scrapy
from scrapy import Request, Spider
import re
from scrapy.selector import Selector, SelectorList
import urllib
from pymongo import MongoClient
import copy
from urllib.parse import urlsplit, parse_qs, urljoin, urlunparse
from CNKI_SPD.utils import dbname_lst
from math import ceil
import time
import json

# import scrapy_redis


sample_headers = {
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Referer': 'http://kns.cnki.net/kcms/detail/detail.aspx?dbcode=CJFD&filename=DHJY202002019&dbname=CJFDLAST2020',
    'Accept-Language': 'zh,zh-CN;q=0.9,en;q=0.8',
}


# todo 把马晟的数据库表复制到我的数据库，机构组织等信息不需要更新，作者在插入前更新一下
# todo 那个网格检索的接口解析掉，东西全在下面的链接，和ref差不多的方式
class SampleSpider(Spider):
    name = 'sample'
    allowed_domains = ['cnki.net']
    base_url = 'kns.cnki.net'
    detail_url = 'kns.cnki.net/kcms/detail/detail.aspx?dbcode={dbcode}&filename={filename}&dbname={dbname}'
    # 电化教育研究	1003-1553  -> DHJY
    start_urls = ['DHJY']

    custom_settings = {
        'DEFAULT_REQUEST_HEADERS': sample_headers
    }

    def start_requests(self):
        pcode = 'CJFD'
        pageIdx = 0
        for pykm in self.start_urls:
            for year in range(2019, 2020):
                for _issue in range(3, 4):
                    issue = str(_issue).rjust(2, '0')
                    # by default
                    url = f'http://navi.cnki.net/knavi/JournalDetail/GetArticleList?year={year}&issue={issue}&pykm={pykm}&pageIdx={pageIdx}&pcode={pcode}'
                    yield Request(
                        url=url,
                        callback=self.get_paper_params,
                        meta={
                            'paper_params': {
                                'pcode': pcode, 'pageIdx': pageIdx, 'pykm': pykm, 'year': year, 'issue': issue
                            }
                        }
                    )

    def get_paper_params(self, response):
        data = response.text
        meta = response.meta
        selector = Selector(response)
        redirect_pages = selector.xpath('//span[@class="name"]/a[1]/@href').getall()
        for page in redirect_pages:
            query = urlsplit(page).query
            params = parse_qs(query)
            transfer_kv_params = lambda d: dict([(key, d[key][0]) for key in d])
            # update response's meta
            params = transfer_kv_params(params)
            params.update(meta['paper_params'])
            # params.update({'paper_params':response.meta['paper_params'])
            yield self.get_paper_req(params)

    def get_paper_req(self, params: dict) -> Request:
        base_paper_url = 'http://kns.cnki.net/kcms/detail/detail.aspx'

        dbcode = params.get('dbCode', '')
        filename = params.get('filename', '')
        dbname = params.get('tableName', '')
        sfield = params.get('FN', '')

        # 只要dbcode和filename即可完成查询
        # url = f'{base_paper_url}?dbcode={dbcode}&filename={filename}'
        url = f'{base_paper_url}?dbcode={dbcode}&filename={filename}&dbname={dbname}'
        # todo 先存在meta里，后面再考虑建个表
        params.update({
            'dbcode': dbcode,
            'filename': filename,
            'dbname': dbname,
            'sfield': sfield,
        })
        return Request(url, callback=self.parse_paper, meta=params)

    def parse_paper(self, response):
        selector = Selector(response)
        meta = response.meta

        # 标题
        title = selector.xpath('//h2[@class="title"]/text()').get()
        author_lst = selector.xpath('//div[@class="author"]//text()').getall()
        # 作者
        author = ' '.join(author_lst)
        author_TurnPageToKnets = selector.xpath('//div[@class="author"]//span/a/@onclick').getall()
        author_ids = []
        for i in author_TurnPageToKnets:
            TurnPageToKnet = re.findall('\'(\d+?)\'', i)
            if TurnPageToKnet:
                author_ids.append(TurnPageToKnet[-1])
        author_ids = " ".join(author_ids)
        orgn_lst = selector.xpath('//div[@class="orgn"]//text()').getall()
        # 机构
        orgn = ' '.join(orgn_lst)
        orgn_TurnPageToKnets = selector.xpath('//div[@class="orgn"]//span/a/@onclick').getall()
        orgn_ids = []
        for i in orgn_TurnPageToKnets:
            orgn_TurnPageToKnet = re.findall('\'(\d+?)\'', i)
            if orgn_TurnPageToKnet:
                orgn_ids.append(orgn_TurnPageToKnet[-1])
        orgn_ids = ' '.join(orgn_ids)
        wxInfo = selector.xpath('//div[@class="wxInfo"]')
        # 摘要
        catalog_ABSTRACT = wxInfo.xpath('.//span[@id="ChDivSummary"]/text()').get()
        # 关键字
        catalog_KEYWORDs = wxInfo.xpath('.//p[label[@id="catalog_KEYWORD"]]//a//text()').getall()
        catalog_KEYWORD = ' '.join([i.strip().strip('\r').strip('\n') for i in catalog_KEYWORDs])
        # DOI
        catalog_ZCDOI = wxInfo.xpath('.//p[label[@id="catalog_ZCDOI"]]/text()').get()

        catalog_FUNDs = wxInfo.xpath('.//p[label[@id="catalog_FUND"]]//a/text()').getall()
        catalog_FUND = []
        for i in catalog_FUNDs:
            catalog_FUND.append(i.strip().strip('\n').strip('\r'))
        catalog_FUND = ' '.join(catalog_FUND)
        FUND_TurnPageToKnets = wxInfo.xpath('.//p[label[@id="catalog_FUND"]]//a/@onclick').getall()
        catalog_FUND_ids = []
        for i in FUND_TurnPageToKnets:
            FUND_TurnPageToKnet = re.findall('\'(\d+?)\'', i)
            if FUND_TurnPageToKnet:
                catalog_FUND_ids.append(FUND_TurnPageToKnet[-1])
        catalog_FUND_ids = ' '.join(catalog_FUND_ids)
        # 分类号
        catalog_ZTCLS = wxInfo.xpath('.//p[label[@id="catalog_ZTCLS"]]/text()').get()
        meta.update({
            'title': title,
            'author': author,
            'author_id': author_ids,
            'orgn': orgn,
            'orgn_id': orgn_ids,
            'url': response.url,
            'catalog_ABSTRACT': catalog_ABSTRACT,
            'catalog_KEYWORD': catalog_KEYWORD,
            'catalog_FUND': catalog_FUND,
            'catalog_FUND_id': catalog_FUND_ids,
            'catalog_ZCDOI': catalog_ZCDOI,
            'catalog_ZTCLS': catalog_ZTCLS
        })
        try:
            # item = copy.copy(meta)
            # item.__setitem__('whichtable', 'insert_author')
            # yield item
            # item.__setitem__('whichtable', 'insert_orgn')
            # yield item
            # item.__setitem__('whichtable', 'insert_fund')
            # yield item
            pass
        except:
            pass
        yield self.knowledge_network(meta)

    # refer_who and who_refer
    def knowledge_network(self, meta: dict):
        dbcode = meta['dbcode']
        dbname = meta['dbname']
        filename = meta['filename']

        # 参考文献 采用list.aspx 不需要 curdbcode
        ref_url = f'http://kns.cnki.net/kcms/detail/frame/list.aspx?dbcode={dbcode}&dbname={dbname}&filename={filename}&reftype=1'
        #
        return Request(url=ref_url, meta=meta, callback=self.parse_kn)

    # todo 检测是否需要翻页 并翻页 注意建立 id 和 url 的关联关系
    def parse_kn(self, response):
        url = response.url
        selector = Selector(response)
        # 以 cjfq 为例 最好用遍历的方式处理
        cjfq_page_cnt = selector.xpath('//span[@id="pc_CJFQ"]/text()').get()
        if cjfq_page_cnt:
            page_num = int(re.search('\d+', cjfq_page_cnt).group())
            #  需要翻页
            if page_num > 10:
                for p in range(1, ceil(page_num / 10) + 1):
                    cjfq_next_url = f'{url}&curdbcode=CJFQ&page={p}'
                    yield Request(url=cjfq_next_url, callback=self.parse_ref, meta=response.meta)
            else:
                yield self.parse_ref(response)

    # 只解析引用文献 调用update mongodb
    def parse_ref(self, response):
        item = response.meta.copy()
        selector = Selector(response)

        href = '//div[@class="essayBox" and .//span[contains(@id,"{}")]]//li/a/@href'
        data = '//div[@class="essayBox" and .//span[contains(@id,"{}")]]//li//text()'
        cjfq_hrefs = selector.xpath(href.format('pc_CJFQ')).getall()
        cjfq_links = [(self.base_url + href).replace('&amp;', '&') for href in cjfq_hrefs]
        cjfq_data_lst = selector.xpath(data.format('pc_CJFQ')).getall()
        s = cjfq_data_lst
        cjfq_str = ''.join(s).replace('\r\n', ' ').replace(' ', '')
        # 解析为标准的ref列表 正则含义：[数字]开头...4位数年份结尾
        # 外文可能会出问题？
        cjfq_ref = re.findall('(\[\d+?\].*?\d{4})', cjfq_str)

        cbbd_hrefs = selector.xpath(href.format('pc_CBBD')).getall()
        cbbd_links = [(self.base_url + href).replace('&amp;', '&') for href in cbbd_hrefs]
        cbbd_data_lst = selector.xpath(data.format('pc_CBBD')).getall()
        s = cbbd_data_lst
        cbbd_str = ''.join(s).replace('\r\n', ' ').replace(' ', '')
        # 解析为标准的ref列表
        cbbd_ref = re.findall('(\[\d+?\].*?\d{4})', cbbd_str)

        ssjd_hrefs = selector.xpath(href.format('pc_SSJD')).getall()
        ssjd_links = [(self.base_url + href).replace('&amp;', '&') for href in ssjd_hrefs]
        ssjd_data_lst = ''.join(selector.xpath(data.format('pc_SSJD')).getall())
        s = ssjd_data_lst
        ssjd_str = ''.join(s).replace('\r\n', ' ').replace(' ', '')
        # 解析为标准的ref列表
        ssjd_ref = re.findall('(\[\d+?\].*?\d{4})', ssjd_str)

        crldeng_hrefs = selector.xpath(href.format('pc_CRLDENG')).getall()
        crldeng_links = [(self.base_url + href).replace('&amp;', '&') for href in crldeng_hrefs]
        crldeng_data_lst = ''.join(selector.xpath(data.format('pc_CRLDENG')).getall())
        s = crldeng_data_lst
        crldeng_str = ''.join(s).replace('\r\n', ' ').replace(' ', '')
        # 解析为标准的ref列表
        crldeng_ref = re.findall('(\[\d+?\].*?\d{4})', crldeng_str)
        item.__setitem__('cjfq_ref', ', '.join(cjfq_ref))
        item.__setitem__('cbbd_ref', ', '.join(cbbd_ref))
        item.__setitem__('ssjd_ref', ', '.join(ssjd_ref))
        item.__setitem__('crldeng_ref', ', '.join(crldeng_ref))

        item.__setitem__('cjfq_links', ', '.join(cjfq_links))
        item.__setitem__('cbbd_links', ', '.join(cbbd_links))
        item.__setitem__('ssjd_links', ', '.join(ssjd_links))
        item.__setitem__('crldeng_links', ', '.join(crldeng_links))

        self.check_item(item)

        item.__setitem__('whichtable', 'insert_item')
        item.__setitem__('download_ts', str(int(time.time())))
        return item

    def check_item(self, item):
        for k in item.keys():
            if isinstance(item[k], list):
                item[k] = str(item[k]) if len(item[k]) else ''
# todo 转义文字为 关联作者
# http://kns.cnki.net/kcms/detail/frame/asynlist.aspx?dbcode=CJFD&dbname=CJFD2000&filename=dhjy200002000&curdbcode=CJFQ&reftype=601&catalogId=lcatalog_func601&catalogName=%E5%85%B3%E8%81%94%E4%BD%9C%E8%80%85%0A%20%20%20%20%20%20%20%20%20%20

# http://kns.cnki.net/kcms/detail/frame/asynlist.aspx?dbcode=CJFD&dbname=CJFDAUTO&filename=DHJY202004004&curdbcode=CJFQ&reftype=1&page=1

# 关联作者  http://kns.cnki.net/kcms/detail/frame/asynlist.aspx?dbcode=CJFD&dbname=CJFD2000&filename=dhjy200002000&curdbcode=CJFQ&reftype=601

# 主题指数 http://kns.cnki.net/kcms/detail/frame/asynlist.aspx?dbcode=CJFD&dbname=CJFDAUTO&filename=DHJY202004004&curdbcode=CJFQ&reftype=602

# 相关基金文献  http://kns.cnki.net/kcms/detail/frame/asynlist.aspx?dbcode=CJFD&dbname=CJFDAUTO&filename=DHJY202004004&curdbcode=CJFQ&reftype=603

# 相似文献  http://kns.cnki.net/kcms/detail/frame/asynlist.aspx?dbcode=CJFD&dbname=CJFDLAST2017&filename=dhjy201708004&curdbcode=CJFQ&reftype=604

# 读者推荐 http://kns.cnki.net/kcms/detail/frame/asynlist.aspx?dbcode=CJFD&dbname=CJFDLAST2017&filename=dhjy201708004&curdbcode=CJFQ&reftype=605

#

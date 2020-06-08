# -*- coding: utf-8 -*-
import scrapy
from scrapy import Request, Spider
import re
from scrapy.selector import Selector, SelectorList
import urllib
from pymongo import MongoClient
import copy
from urllib.parse import urlsplit, parse_qs, urljoin, urlunparse
from math import ceil
import time
from datetime import datetime
import json

sample_headers = {
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Language': 'zh,zh-CN;q=0.9,en;q=0.8',
}


# todo 那个网格检索的接口解析掉，东西全在下面的链接，和ref差不多的方式
class SampleSpider(Spider):
    name = 'sample'
    allowed_domains = ['cnki.net']
    base_url = 'kns.cnki.net'
    detail_url = 'kns.cnki.net/kcms/detail/detail.aspx?dbcode={dbcode}&filename={filename}&dbname={dbname}'
    # 电化教育研究	1003-1553  -> DHJY
    start_urls = ['DHJY']
    pcode = 'CJFD'
    start_year = 2010
    end_year = 2011
    start_issue = 5
    end_issue = 6
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
                    # by default
                    url = f'http://navi.cnki.net/knavi/JournalDetail/GetArticleList?year={year}&issue={issue}&pykm={pykm}&pageIdx={pageIdx}&pcode={pcode}'
                    yield Request(
                        headers=sample_headers,
                        url=url,
                        callback=self.get_paper_params,
                        meta={
                            'paper_params': {
                                'pcode': pcode, 'pageIdx': pageIdx, 'pykm': pykm, 'year': year, 'issue': issue
                            }
                        }
                    )

    def get_paper_params(self, response):
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
        return Request(headers=sample_headers, method='GET', url=url, callback=self.parse_paper, meta=params)

    def parse_paper(self, response):
        selector = Selector(response)
        meta = response.meta
        if meta['filename'].endswith('5008'):
            print(meta)
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
            'catalog_ABSTRACT': catalog_ABSTRACT if catalog_ABSTRACT else '',
            'catalog_KEYWORD': catalog_KEYWORD if catalog_KEYWORDs else '',
            'catalog_FUND': catalog_FUND if catalog_FUND else '',
            'catalog_FUND_id': catalog_FUND_ids if catalog_FUND_ids else '',
            'catalog_ZCDOI': catalog_ZCDOI if catalog_ZCDOI else '',
            'catalog_ZTCLS': catalog_ZTCLS if catalog_ZTCLS else ''
        })
        return self.knowledge_network(meta)

    # refer_who and who_refer
    def knowledge_network(self, meta: dict):
        dbcode = meta['dbcode']
        dbname = meta['dbname']
        filename = meta['filename']

        # 参考文献 采用list.aspx 不需要 curdbcode
        ref_url = f'http://kns.cnki.net/kcms/detail/frame/list.aspx?dbcode={dbcode}&dbname={dbname}&filename={filename.lower()}&RefType=1&vl='
        #
        headers = {
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-User': '?1',
            'Sec-Fetch-Dest': 'document',
            'Referer': 'https://kns.cnki.net/kcms/detail/detail.aspx?dbcode=CJFD&filename=DHJY201903004&dbname=CJFDLAST2019',
            'Accept-Language': 'zh,zh-CN;q=0.9,en;q=0.8',
        }
        headers['Referer'] = meta['url']
        item = meta.copy()
        item['whichtable'] = 'Item'
        self.check_item(item)
        yield item
        return Request(method='GET', url=ref_url, headers=headers, callback=self.parse_kn, cb_kwargs={'value': meta})

    def parse_kn(self, response, value):
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
                    yield Request(url=cjfq_next_url, callback=self.parse_ref, cb_kwargs={'value': value})
            else:
                self.parse_ref(response, value)
        else:
            value['whichtable'] = 'Item'
            self.check_item(value)
            yield value

    # 只解析引用文献
    def parse_ref(self, response, value):
        selector = Selector(response)
        refs_xpath = '//div[@class="essayBox" and .//span[contains(@id,"{}")]]//ul//li'
        ref_ids = ['pc_CJFQ', 'pc_CBBD', 'pc_SSJD', 'pc_CRLDENG']
        for ref_i in ref_ids:
            for i in selector.xpath(refs_xpath.format(ref_i)):
                href = i.xpath('./a[1]/@href').get()
                data = i.xpath('string(.)').get()
                query = urlsplit(href).query
                ret = parse_qs(query)
                # 字段可能为空
                if ret.get('filename'):
                    ref_filename = ret['filename'][0]
                else:
                    ref_filename = ''
                if ret.get('dbcode'):
                    ref_dbcode = ret['dbcode'][0]
                else:
                    ref_dbcode = ''
                if ret.get('dbname'):
                    ref_dbname = ret['dbname'][0]
                else:
                    ref_dbname = ''
                item = {'whichtable': 'Filename', 'filename': value['filename'], 'dbcode': value['dbcode'],
                        'dbname': value['dbname'], 'extra': value['title']}
                self.check_item(item)
                yield item
                # todo skip following code to debug
                item = {'whichtable': 'Filename', 'filename': ref_filename, 'dbcode': ref_dbcode, 'dbname': ref_dbname,
                        'extra': data}
                self.check_item(item)
                yield item
                # Ref

                item = {'whichtable': 'Ref', 'citing_filename': value['filename'], 'cited_filename': ref_filename}
                self.check_item(item)
                yield item
        value['whichtable'] = 'Item'
        self.check_item(value)
        yield value

    def check_item(self, item):
        for k in item.keys():
            if isinstance(item[k], list):
                item[k] = json.dumps(item[k]) if len(item[k]) else ''
        item['download_ts'] = datetime.isoformat(datetime.now())
# todo 转义文字为 关联作者
# http://kns.cnki.net/kcms/detail/frame/asynlist.aspx?dbcode=CJFD&dbname=CJFD2000&filename=dhjy200002000&curdbcode=CJFQ&reftype=601&catalogId=lcatalog_func601&catalogName=%E5%85%B3%E8%81%94%E4%BD%9C%E8%80%85%0A%20%20%20%20%20%20%20%20%20%20

# http://kns.cnki.net/kcms/detail/frame/asynlist.aspx?dbcode=CJFD&dbname=CJFDAUTO&filename=DHJY202004004&curdbcode=CJFQ&reftype=1&page=1

# 关联作者  http://kns.cnki.net/kcms/detail/frame/asynlist.aspx?dbcode=CJFD&dbname=CJFD2000&filename=dhjy200002000&curdbcode=CJFQ&reftype=601

# 主题指数 http://kns.cnki.net/kcms/detail/frame/asynlist.aspx?dbcode=CJFD&dbname=CJFDAUTO&filename=DHJY202004004&curdbcode=CJFQ&reftype=602

# 相关基金文献  http://kns.cnki.net/kcms/detail/frame/asynlist.aspx?dbcode=CJFD&dbname=CJFDAUTO&filename=DHJY202004004&curdbcode=CJFQ&reftype=603

# 相似文献  http://kns.cnki.net/kcms/detail/frame/asynlist.aspx?dbcode=CJFD&dbname=CJFDLAST2017&filename=dhjy201708004&curdbcode=CJFQ&reftype=604

# 读者推荐 http://kns.cnki.net/kcms/detail/frame/asynlist.aspx?dbcode=CJFD&dbname=CJFDLAST2017&filename=dhjy201708004&curdbcode=CJFQ&reftype=605

#

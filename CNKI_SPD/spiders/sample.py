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
    start_issue = 1
    end_issue = 3
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
            params = transfer_kv_params(params)
            params.update(meta['paper_params'])
            yield self.get_paper_req(params)

    def get_paper_req(self, params: dict) -> Request:
        base_paper_url = 'http://kns.cnki.net/kcms/detail/detail.aspx'
        dbcode = params.get('dbCode', '')
        filename = params.get('filename', '')
        dbname = params.get('tableName', '')
        sfield = params.get('FN', '')

        url = f'{base_paper_url}?dbcode={dbcode}&filename={filename}&dbname={dbname}'
        params.update({
            'dbcode': dbcode,
            'filename': filename,
            'dbname': dbname,
            'sfield': sfield,
        })
        return Request(headers=sample_headers, method='GET', url=url, callback=self.parse_paper, meta=params)

    def parse_paper(self, response, whichtable='', Item_filename=''):
        selector = Selector(response)
        meta = response.meta
        navi_link_issue = selector.xpath('//div[@class="sourinfo"]/p[3]/a/@onclick').get()
        try:
            if navi_link_issue:
                pcode = navi_link_issue.split(',')[1].strip('\'')
                pykm = navi_link_issue.split(',')[3].strip('\'')
                year = navi_link_issue.split(',')[4].strip('\'')
                issue = navi_link_issue.split(',')[5].strip('\'')
        except:
            pcode = pykm = year = issue = ''
        issue = issue.rstrip('\')')
        ISSN = selector.xpath('//div[@class="sourinfo"]/p[4]/text()').get()
        if ISSN:
            ISSN = ISSN.replace('ISSN', '').lstrip('\r\n:： ')
        else:
            ISSN = ''
        # 标题
        title = selector.xpath('//h2[@class="title"]/text()').get()
        # 作者
        author_lst = selector.xpath('//div[@class="author"]//text()').getall()
        author_TurnPageToKnets = selector.xpath('//div[@class="author"]//span/a/@onclick').getall()
        author_ids = []
        for i in author_TurnPageToKnets:
            TurnPageToKnet = re.findall('\'(\d+?)\'', i)
            if TurnPageToKnet:
                author_ids.append(TurnPageToKnet[-1])
        # 可能author 没有id 需要验证
        if len(author_lst) != len(author_ids):
            for index, i in enumerate(author_lst):
                for j in author_TurnPageToKnets:
                    if i in j:
                        break
                else:
                    author_ids.insert(index, get_md5())
        author = ' '.join(author_lst)
        author_ids = ' '.join(author_ids)
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

        catalog_FUNDs = wxInfo.xpath('.//p[label[@id="catalog_FUND"]]//a')
        FUND = []
        for i in catalog_FUNDs:
            FUND.append(i.xpath('string(.)').get())
        catalog_FUND = ' '.join(FUND)

        FUND_ids = []
        for i in FUND:
            F_id = re.search(':?\(?([0-9a-zA-Z-/]+?)\)', i)
            if F_id:
                FUND_ids.append(F_id.group(1))
            else:
                FUND_ids.append(get_md5())
        catalog_FUND_ids = ' '.join(FUND_ids)
        # 分类号
        catalog_ZTCLS = wxInfo.xpath('.//p[label[@id="catalog_ZTCLS"]]/text()').get()
        meta.update({
            'pcode': pcode,
            'pykm': pykm,
            'year': year,
            'issue': issue,
            'ISSN': ISSN,
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
        # 保存Item详细信息
        item = meta.copy()
        if whichtable:
            item['whichtable'] = whichtable
            query = urlsplit(response.url).query
            filename = parse_qs(query)['filename'][0]
            dbname = parse_qs(query)['dbname'][0]
            dbcode = parse_qs(query)['dbcode'][0]
            item['filename'] = filename
            item['dbname'] = dbname
            item['dbcode'] = dbcode
            self.check_item(item)
            yield item
            rel_item = {'Ref_Info_filename': filename}
            rel_item['whichtable'] = 'Ref_Rel'
            rel_item['Item_filename'] = Item_filename
            self.check_item(rel_item)
            yield rel_item
        else:
            item['whichtable'] = 'Item'
            self.check_item(item)
            yield item
            yield self.knowledge_network(meta)


# refer_who and who_refer
def knowledge_network(self, meta: dict):
    dbcode = meta['dbcode']
    dbname = meta['dbname']
    filename = meta['filename']

    # 参考文献 采用list.aspx 不需要 curdbcode
    ref_url = f'http://kns.cnki.net/kcms/detail/frame/list.aspx?dbcode={dbcode}&dbname={dbname}&filename={filename.lower()}&RefType=1&vl='
    #
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh,zh-CN;q=0.9,en;q=0.8',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Host': 'kns.cnki.net',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36',
        'Upgrade-Insecure-Requests': '1',
        'Referer': 'https://kns.cnki.net/kcms/detail/detail.aspx?dbcode=CJFD&filename=DHJY201005002&dbname=CJFD2010',
    }
    headers['Referer'] = meta['url']

    return Request(url=ref_url, headers=headers, callback=self.parse_kn, cb_kwargs={'value': meta})


def parse_kn(self, response, value):
    url = response.url
    selector = Selector(response)
    # 以 cjfq 为例 最好用遍历的方式处理
    ref_ids = ['pc_CJFQ', 'pc_CBBD', 'pc_SSJD']
    # ref_ids = ['pc_CJFQ', 'pc_CBBD', 'pc_SSJD', 'pc_CRLDENG']
    for ref_i in ref_ids:
        value['ref_type'] = ref_i
        page_cnt = selector.xpath(f'//span[@id="{ref_i}"]/text()').get()
        if page_cnt:
            page_num = int(re.search('\d+', page_cnt).group())
            #  需要翻页
            if page_num <= 10:
                for _item in self.parse_ref(response, value):
                    yield _item
            else:
                if page_num % 10:
                    max_page = ceil(page_num / 10)
                else:
                    max_page = page_num // 10
                    for p in range(1, max_page + 1):
                        next_url = f'{url}&curdbcode=CJFQ&page={p}'
                        yield Request(url=next_url, callback=self.parse_ref, cb_kwargs={'value': value})


# 只解析引用文献
def parse_ref(self, response, value):
    selector = Selector(response)
    refs_xpath = '//div[@class="essayBox" and .//span[contains(@id,"{}")]]//ul//li'
    ref_i = value.get('ref_i', '')
    for i in selector.xpath(refs_xpath.format(ref_i)):
        href = i.xpath('./a[1]/@href').get()
        data = i.xpath('string(.)').get()
        if data:
            data = re.sub('\[\d+?\]', '', data)
        if href:
            # 有新链接，处理Ref_Info 同时记录Ref_
            next_url = 'http://' + self.base_url + href
            yield Request(headers=sample_headers, url=next_url, callback=self.parse_paper,
                          cb_kwargs={'whichtable': 'Ref_Info', 'Item_filename': value['filename']})
        elif data:
            # 没有链接，只有文本，把文本切分成基本信息载入Ref_Info
            item = {'filename': get_md5()}
            self.split_reference(item, data)
            item['whichtable'] = 'Ref_Info'
            item['easy_insert'] = 'yes'
            self.check_item(item)
            yield item
            # 同时记录 Ref_Rel
            rel_item = {'Ref_Info_filename': get_md5()}
            rel_item['Item_filename'] = value['filename']
            rel_item['whichtable'] = 'Ref_Rel'
            self.check_item(rel_item)
            yield rel_item


def check_item(self, item):
    for k in item.keys():
        if isinstance(item[k], list):
            item[k] = json.dumps(item[k]) if len(item[k]) else ''
        elif isinstance(item[k], str):
            item[k] = item[k].strip('\r\n ')
    item['download_ts'] = datetime.isoformat(datetime.now())


def split_reference(self, item, data):
    data = data.replace('\r\n', ' ').replace('&nbsp&nbsp', ' ').replace(',', '.')
    data_lst = data.split('.')
    title = data_lst[0]
    author = data_lst[1]
    author_id = get_md5()
    orgn = data_lst[2]
    orgn_id = get_md5()
    year = data_lst[-1]
    item['title'] = title
    item['author'] = author
    item['author_id'] = author_id
    item['orgn'] = orgn
    item['orgn_id'] = orgn_id
    item['fund'] = ''
    item['fund_id'] = ''
    item['year'] = year

# todo 转义文字为 关联作者
# http://kns.cnki.net/kcms/detail/frame/asynlist.aspx?dbcode=CJFD&dbname=CJFD2000&filename=dhjy200002000&curdbcode=CJFQ&reftype=601&catalogId=lcatalog_func601&catalogName=%E5%85%B3%E8%81%94%E4%BD%9C%E8%80%85%0A%20%20%20%20%20%20%20%20%20%20

# http://kns.cnki.net/kcms/detail/frame/asynlist.aspx?dbcode=CJFD&dbname=CJFDAUTO&filename=DHJY202004004&curdbcode=CJFQ&reftype=1&page=1

# 关联作者  http://kns.cnki.net/kcms/detail/frame/asynlist.aspx?dbcode=CJFD&dbname=CJFD2000&filename=dhjy200002000&curdbcode=CJFQ&reftype=601

# 主题指数 http://kns.cnki.net/kcms/detail/frame/asynlist.aspx?dbcode=CJFD&dbname=CJFDAUTO&filename=DHJY202004004&curdbcode=CJFQ&reftype=602

# 相关基金文献  http://kns.cnki.net/kcms/detail/frame/asynlist.aspx?dbcode=CJFD&dbname=CJFDAUTO&filename=DHJY202004004&curdbcode=CJFQ&reftype=603

# 相似文献  http://kns.cnki.net/kcms/detail/frame/asynlist.aspx?dbcode=CJFD&dbname=CJFDLAST2017&filename=dhjy201708004&curdbcode=CJFQ&reftype=604

# 读者推荐 http://kns.cnki.net/kcms/detail/frame/asynlist.aspx?dbcode=CJFD&dbname=CJFDLAST2017&filename=dhjy201708004&curdbcode=CJFQ&reftype=605

#

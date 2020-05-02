# -*- coding: utf-8 -*-
import os
import sys
sys.path.append(os.path.abspath('..'))

import scrapy
from scrapy import Request, Spider
from scrapy.selector import Selector, SelectorList
import urllib
from urllib.parse import urlsplit, parse_qs, urljoin, urlunparse
from CNKI_SPD.utils import dbname_lst

# import scrapy_redis


sample_headers = {
  'Accept': ' text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
  'Accept-Encoding': ' gzip, deflate',
  'Accept-Language': ' zh,zh-CN;q=0.9,en;q=0.8',
  'Cache-Control': ' max-age=0',
  'Connection': ' keep-alive'
}


class SampleSpider(Spider):
    name = 'sample'
    allowed_domains = ['cnki.net']
    # 电化教育研究	1003-1553  -> DHJY
    start_urls = ['DHJY']

    custom_settings = {
        'DEFAULT_REQUEST_HEADERS':sample_headers
    }

    def start_requests(self):
        pcode = 'CJFD'
        pageIdx = 0
        for pykm in self.start_urls:
            for year in range(2020, 2021):
                for _issue in range(4, 5):
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
            paper_req = self.get_paper_req(params)
            yield paper_req

        # print(data[:200])

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
        orgn_lst = selector.xpath('//div[@class="orgn"]//text()').getall()
        # 机构
        orgn = ' '.join(orgn_lst)

        wxInfo = selector.xpath('//div[@class="wxInfo"]')
        # 摘要
        catalog_ABSTRACT = wxInfo.xpath('.//span[@id="ChDivSummary"]/text()').get()
        # 关键字
        catalog_KEYWORD = wxInfo.xpath('.//p[label[@id="catalog_KEYWORD"]]//a//text()').get()
        # DOI
        catalog_ZCDOI = wxInfo.xpath('.//p[label[@id="catalog_ZCDOI"]]/text()').get()
        # 分类号
        catalog_ZTCLS = wxInfo.xpath(('.//p[label[@id="catalog_ZTCLS"]]/text()')).get()
        meta.update({
            'title':title,
            'author':author,
            'orgn':orgn,
            'catalog_ABSTRACT':catalog_ABSTRACT,
            'catalog_KEYWORD':catalog_KEYWORD,
            'catalog_ZCDOI':catalog_ZCDOI,
            'catalog_ZTCLS':catalog_ZTCLS
        })
        return self.knowledge_network(meta)


    # refer_who and who_refer
    def knowledge_network(self, meta: dict):
        dbcode = meta['dbcode']
        dbname = meta['dbname']
        filename = meta['filename']

        # for c_db in dbname_lst:
        #     cur_dbcode=c_db['id']
        #     cur_dbname=c_db['dbname']
        #
        #     # 参考文献
        #     ref_url = f'http://kns.cnki.net/kcms/detail/frame/asynlist.aspx?dbcode={dbcode}&dbname={dbname}&filename={filename}&curdbcode={cur_dbcode}&reftype=1'
        #     yield Request(url=ref_url,meta=meta,callback=self.parse_kn)
            # 相关基金文献

        # todo 全网期刊
        cur_dbcode = 'CJFQ'

        # 参考文献 采用list.aspx
        ref_url = f'http://kns.cnki.net/kcms/detail/frame/asynlist.aspx?dbcode={dbcode}&dbname={dbname}&filename={filename}&curdbcode={cur_dbcode}&reftype=1'
        yield Request(url=ref_url, meta=meta, callback=self.parse_kn)


    # todo 检测是否需要翻页 并翻页 注意建立 id 和 url 的关联关系
    def parse_kn(self,response):



        item=response.meta.copy()
        selector=Selector(response)
        raw_kn=selector.xpath('//div[@class="essayBox"]//text()').getall()
        if len(raw_kn) and isinstance(raw_kn,list):
            item['ref_wx']=''.join(raw_kn)
        else:
            item['ref_wx']=''
        print(item['ref_wx'])
        yield item





# todo 转义文字为 关联作者
# http://kns.cnki.net/kcms/detail/frame/asynlist.aspx?dbcode=CJFD&dbname=CJFD2000&filename=dhjy200002000&curdbcode=CJFQ&reftype=601&catalogId=lcatalog_func601&catalogName=%E5%85%B3%E8%81%94%E4%BD%9C%E8%80%85%0A%20%20%20%20%20%20%20%20%20%20

# todo 参考文献 page={page} http://kns.cnki.net/kcms/detail/frame/list.aspx?dbcode=CJFD&dbname=CJFDAUTO&filename=DHJY202004004&curdbcode=CJFQ&reftype=1&page=1
# http://kns.cnki.net/kcms/detail/frame/asynlist.aspx?dbcode=CJFD&dbname=CJFDAUTO&filename=DHJY202004004&curdbcode=CJFQ&reftype=1&page=1

# 关联作者  http://kns.cnki.net/kcms/detail/frame/asynlist.aspx?dbcode=CJFD&dbname=CJFD2000&filename=dhjy200002000&curdbcode=CJFQ&reftype=601

# 主题指数 http://kns.cnki.net/kcms/detail/frame/asynlist.aspx?dbcode=CJFD&dbname=CJFDAUTO&filename=DHJY202004004&curdbcode=CJFQ&reftype=602

# 相关基金文献  http://kns.cnki.net/kcms/detail/frame/asynlist.aspx?dbcode=CJFD&dbname=CJFDAUTO&filename=DHJY202004004&curdbcode=CJFQ&reftype=603

#  相似文献  http://kns.cnki.net/kcms/detail/frame/asynlist.aspx?dbcode=CJFD&dbname=CJFDLAST2017&filename=dhjy201708004&curdbcode=CJFQ&reftype=604

# 读者推荐 http://kns.cnki.net/kcms/detail/frame/asynlist.aspx?dbcode=CJFD&dbname=CJFDLAST2017&filename=dhjy201708004&curdbcode=CJFQ&reftype=605




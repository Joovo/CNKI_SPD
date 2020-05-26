# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
from CNKI_SPD.DBHelper import MongoDB
import pymysql
from . import settings

# todo 去重
class CnkiSpdPipeline(object):

    def process_item(self, item, spider):

        # 移除 scrapy 抓取时产生的参数
        del item['depth']
        del item['download_slot']
        del item['download_latency']

        # useless
        del item['sfield']
        # duplicated
        del item['dbCode']
        del item['tableName']

        conn = pymysql.connect(host=settings.mysql_host, user=settings.mysql_user, password=settings.mysql_password,
                               use_unicode=True, charset='utf8mb4', db='JH_CNKI')
        with conn.cursor() as cursor:
            # check exist & create database
            if not cursor.execute('show databases like "JH_CNKI";'):
                cursor.execute('create database JH_CNKI;')
            cursor.execute('use JH_CNKI;')
            if not cursor.execute('show tables like "item"'):
                sql = """
                create table item(
                    _id int auto_increment primary key,
                    dbname varchar(255) not null,
                    filename varchar(255) not null,
                    pcode varchar(20),
                    pageIdx int(11),
                    pykm varchar(20),
                    year varchar(20),
                    issue varchar(20),
                    dbcode varchar(20),
                    download_timeout varchar(50),
                    title longtext,
                    author longtext,
                    orgn longtext,
                    url longtext,
                    catalog_ABSTRACT longtext,
                    catalog_KEYWORD longtext,
                    catalog_ZCDOI varchar(255),
                    catalog_ZTCLS varchar(255),
                    cjfq_ref longtext,
                    cbbd_ref longtext,
                    ssjd_ref longtext,
                    crldeng_ref longtext,
                    cjfq_links longtext,
                    cbbd_links longtext,
                    ssjd_links longtext,
                    crldeng_links longtext     
                ) default charset=utf8;
                """
                cursor.execute(sql)
                cursor.commit()
        with conn.cursor() as cursor:
            try:
                insert_item_sql = f'''insert into `item` (`dbname`,`filename`,`pcode`,`pageIdx`,`pykm`,`year`,`issue`,`dbcode`,`download_timeout`,`title`,`author`,`orgn`,`url`,`catalog_ABSTRACT`,`catalog_KEYWORD`,`catalog_ZCDOI`,`catalog_ZTCLS`,`cjfq_ref`,`cbbd_ref`,`ssjd_ref`,`crldeng_ref`,`cjfq_links`,`cbbd_links`,`ssjd_links`,`crldeng_links`) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) '''

                cursor.execute(insert_item_sql, (
                    item['dbname'], item['filename'], item['pcode'], item['pageIdx'], item['pykm'], item['year'],
                    item['issue'], item['dbcode'], item['download_timeout'], item['title'], item['author'],
                    item['orgn'], item['url'], item['catalog_ABSTRACT'], item['catalog_KEYWORD'],
                    item['catalog_ZCDOI'], item['catalog_ZTCLS'], item['cjfq_ref'], item['cbbd_ref'],
                    item['ssjd_ref'], item['crldeng_ref'], item['cjfq_links'], item['cbbd_links'], item['ssjd_links'],
                    item['crldeng_links']))
                conn.commit()

            except Exception as e:
                conn.rollback()

        conn.close()
        return item


class CnkiSpdMongoDBPipeline(object):
    def process_item(self, item, spider):
        mongo = MongoDB().connect()
        result = mongo.CNKI.item
        try:
            # 移除 scrapy 抓取时产生的参数
            del item['depth']
            del item['download_slot']
            del item['download_latency']

            # useless
            del item['sfield']
            # duplicated
            del item['dbCode']
            del item['tableName']

            result.update_one(
                {
                    'filename': item['filename'],
                    'dbname': item['dbname']
                },
                {
                    '$set': item
                }, upsert=True
            )
        except:
            print(f'CnkiSpdPipeline Error. data={str(item)}')
            errorItem = mongo.CKNI.errorItem
            errorItem.insert_one(item)
        return item

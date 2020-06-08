# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql
from . import settings
import threading
from contextlib import contextmanager


@contextmanager
def create_conn(*args, **kwargs):
    _conn = pymysql.connect(host=settings.mysql_host, user=settings.mysql_user, password=settings.mysql_password,
                            use_unicode=True, charset='utf8mb4', db='JH_CNKI')
    try:
        yield _conn
    finally:
        _conn.commit()
        _conn.close()


class CnkiSpdPipeline(object):
    def process_item(self, item, spider):
        # 移除 scrapy 抓取时产生的参数
        try:
            del item['depth']
            del item['download_slot']
            del item['download_latency']

            # useless
            del item['sfield']
            # duplicated
            del item['dbCode']
            del item['tableName']
        except:
            pass
        # 建数据库
        with create_conn() as conn:
            with conn.cursor() as cursor:
                # check exist & create database
                if not cursor.execute('show databases like "JH_CNKI";'):
                    cursor.execute('create database JH_CNKI;')
                cursor.execute('use JH_CNKI;')
                conn.commit()

        if item['whichtable'] == 'Filename':
            return self.insert_filename(item)
        elif item['whichtable'] == 'Ref':
            return self.insert_ref(item)
        elif item['whichtable'] == 'Item':
            self.insert_author(item)
            self.insert_orgn(item)
            self.insert_fund(item)
            return self.insert_item(item)

    def insert_filename(self, item) -> dict:
        with create_conn() as conn:
            with conn.cursor() as cursor:
                select_sql = '''show tables like "Filename"'''
                if not cursor.execute(select_sql):
                    cursor.execute('''create table `Filename`(
                        _id int auto_increment primary key,
                        filename varchar(255) unique ,
                        dbcode varchar(255),
                        dbname varchar(255),
                        extra longtext comment "一般是 title 或者是被引时导出信息",
                        download_ts varchar(255)
                    )default charset=utf8;''')
                    conn.commit()
            with conn.cursor() as cursor:
                distinct_sql = '''select filename from `Filename` where filename=%s or extra=%s'''
                if cursor.execute(distinct_sql, (item['filename'], item['extra'])):
                    return item
            with conn.cursor() as cursor:
                insert_sql = '''insert into `Filename` (`filename`,`dbcode`,`dbname`,`extra`,`download_ts`) values (%s,%s,%s,%s,%s)'''
                cursor.execute(insert_sql,
                               (item['filename'], item['dbcode'], item['dbname'], item['extra'], item['download_ts']))
                conn.commit()

    def insert_ref(self, item) -> dict:
        with create_conn() as conn:
            with conn.cursor() as cursor:
                select_sql = '''show tables like "Ref"'''
                if not cursor.execute(select_sql):
                    cursor.execute('''create table `Ref`(
                        _id int auto_increment primary key,
                        citing_id varchar(255),
                        citing_filename varchar(255),
                        cited_id varchar(255),
                        cited_filename varchar(255),
                        download_ts varchar(255)
                    )default charset=utf8;''')
                    conn.commit()
            with conn.cursor() as cursor:
                select_sql = '''select _id from `Filename` where `filename`=%s'''
                cursor.execute(select_sql, item['citing_filename'])
                conn.commit()
                ret = cursor.fetchall()
                citing_id = ret[0][0]
                select_sql = '''select _id from `Filename` where `filename`=%s'''
                cursor.execute(select_sql, item['cited_filename'])
                conn.commit()
                ret = cursor.fetchall()
                cited_id = ret[0][0]
            with conn.cursor() as cursor:
                dinctinct_sql = '''select `citing_filename`,`cited_filename` from `Ref` where `citing_filename`=%s and `cited_filename`=%s'''
                if cursor.execute(dinctinct_sql, (item['citing_filename'], item['cited_filename'])):
                    return item
            with conn.cursor() as cursor:

                insert_sql = '''insert into `Ref` (`citing_id`,`citing_filename`,`cited_id`,`cited_filename`,`download_ts`) values(%s,%s,%s,%s,%s);'''
                cursor.execute(insert_sql, (
                    citing_id, item['citing_filename'], cited_id, item['cited_filename'], item['download_ts']))
                conn.commit()

    def insert_author(self, item) -> dict:
        with create_conn() as conn:
            with conn.cursor() as cursor:
                if not cursor.execute('show tables like "Author"'):
                    sql = """create table `Author`(
                    _id int auto_increment primary key,
                    author longtext not null,
                    author_id varchar(255),
                    download_ts varchar(255)
                    )default charset=utf8;"""
                    cursor.execute(sql)
                    conn.commit()

            with conn.cursor() as cursor:
                au = item['author'].split()
                au_id = item['author_id'].split()
                try:
                    for i in range(len(au)):
                        select_author_sql = """select * from `Author` where `author`=%s"""
                        if not cursor.execute(select_author_sql, au[i]):
                            insert_author_sql = """insert into `Author` (`author`,`author_id`,`download_ts`) values (%s,%s,%s)"""
                            cursor.execute(insert_author_sql, (au[i], au_id[i], item['download_ts']))
                            conn.commit()
                except Exception as e:
                    conn.rollback()
                    conn.commit()

    def insert_orgn(self, item) -> dict:
        with create_conn() as conn:
            with conn.cursor() as cursor:
                if not cursor.execute('show tables like "Orgn"'):
                    sql = """create table `Orgn`(
                    _id int auto_increment primary key,
                    orgn longtext not null,
                    orgn_id varchar(255),
                    download_ts varchar(255)
                    )default charset=utf8;"""
                    cursor.execute(sql)
                    conn.commit()
            with conn.cursor() as cursor:
                try:
                    og = item['orgn'].split()
                    og_id = item['orgn_id'].split()
                    for i in range(len(og)):
                        select_orgn_sql = """select * from `Orgn` where `orgn`=%s"""
                        if not cursor.execute(select_orgn_sql, og[i]):
                            insert_orgn_sql = """insert into `Orgn` (`orgn`,`orgn_id`,`download_ts`) values (%s,%s,%s)"""
                            # list index out of range
                            if i >= len(og_id):
                                cursor.execute(insert_orgn_sql, (og[i], '', item['download_ts']))
                            else:
                                cursor.execute(insert_orgn_sql, (og[i], og_id[i], item['download_ts']))
                            conn.commit()
                except Exception as e:
                    conn.rollback()
                    conn.commit()

    def insert_fund(self, item) -> dict:
        with create_conn() as conn:
            with conn.cursor() as cursor:
                if not cursor.execute('show tables like "Fund"'):
                    sql = """create table `Fund`(
                    _id int auto_increment primary key,
                    fund longtext not null,
                    fund_id varchar(255),
                    download_ts varchar(255)
                    )default charset=utf8;"""
                    cursor.execute(sql)
                    conn.commit()

            with conn.cursor() as cursor:
                try:
                    fd = item['catalog_FUND'].split()
                    fd_id = item['catalog_FUND_id'].split()
                    for i in range(len(fd)):
                        select_fund_sql = """select * from `Fund` where `fund`=%s"""
                        if not cursor.execute(select_fund_sql, fd[i]):
                            insert_fund_sql = """insert into `Fund` (`fund`,`fund_id`,`download_ts`) values (%s,%s,%s)"""
                            cursor.execute(insert_fund_sql, (fd[i], fd_id[i], item['download_ts']))
                            conn.commit()
                except Exception as e:
                    conn.rollback()
                    conn.commit()

    def insert_item(self, item):
        with create_conn() as conn:
            # 创建详情表
            with conn.cursor() as cursor:
                if not cursor.execute('show tables like "Item"'):
                    sql = """create table `Item`(
                        _id int auto_increment primary key,
                        dbname varchar(255) not null,
                        filename varchar(255) not null unique,
                        pcode varchar(20),
                        pageIdx int(11),
                        pykm varchar(20),
                        year varchar(20),
                        issue varchar(20),
                        dbcode varchar(20),
                        download_timeout varchar(50),
                        title longtext,
                        author longtext,
                        author_id longtext,
                        orgn longtext,
                        orgn_id longtext,
                        url longtext,
                        catalog_ABSTRACT longtext,
                        catalog_KEYWORD longtext,
                        catalog_ZCDOI varchar(255),
                        catalog_FUND longtext,
                        catalog_FUND_id longtext,
                        catalog_ZTCLS varchar(255)  
                    ) default charset=utf8;"""
                    cursor.execute(sql)
                    conn.commit()
            # 插入详情表
            with conn.cursor() as cursor:
                try:
                    select_item_sql = f'''select `filename` from `Item` where `filename`=%s'''
                    if cursor.execute(select_item_sql, item['filename']):
                        return item
                    else:
                        insert_item_sql = f'''insert into `Item` (`dbname`,`filename`,`pcode`,`pageIdx`,`pykm`,`year`,`issue`,`dbcode`,`download_timeout`,`title`,`author`,`author_id`,`orgn`,`orgn_id`,`url`,`catalog_ABSTRACT`,`catalog_KEYWORD`,`catalog_ZCDOI`,`catalog_FUND`,`catalog_FUND_id`,`catalog_ZTCLS`) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'''

                        cursor.execute(insert_item_sql, (
                            item['dbname'], item['filename'], item['pcode'], item['pageIdx'], item['pykm'],
                            item['year'],
                            item['issue'], item['dbcode'], item['download_timeout'], item['title'], item['author'],
                            item['author_id'],
                            item['orgn'], item['orgn_id'], item['url'], item['catalog_ABSTRACT'],
                            item['catalog_KEYWORD'],
                            item['catalog_ZCDOI'], item['catalog_FUND'], item['catalog_FUND_id'], item['catalog_ZTCLS']
                        ))
                        conn.commit()
                except Exception as e:
                    conn.rollback()
                    conn.commit()
            return item

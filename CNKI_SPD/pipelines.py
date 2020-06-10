# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql
from . import settings
import threading
from .utils import get_md5
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
        # todo item->ref_info,citing_info,item priority equal to item
        # todo Ref->ref_rel,citing_rel
        if item['whichtable'] in ('Item', 'Ref_Info', 'Citing_Info'):
            self.insert_author(item)
            self.insert_orgn(item)
            self.insert_fund(item)
            return self.insert_item(item)
        elif item['whichtable'] == 'Ref_Rel':
            return self.insert_ref_rel(item)
        elif item['whichtable'] == 'Citing_Rel':
            return self.insert_citing_rel(item)

    def insert_citing_rel(self, item):
        pass

    # 改造为 ref_rel
    def insert_ref_rel(self, item) -> dict:
        with create_conn() as conn:
            with conn.cursor() as cursor:
                select_sql = '''show tables like "Ref_Rel"'''
                if not cursor.execute(select_sql):
                    cursor.execute('''create table `Ref_Rel`(
                        _id int auto_increment primary key,
                        Ref_Info_filename varchar(255),
                        Item_filename varchar(255),
                        download_ts varchar(255)
                    )default charset=utf8;''')
                    conn.commit()
            with conn.cursor() as cursor:
                dinctinct_sql = '''select `Ref_Info_filename`,`Item_filename` from `Ref_Rel` where `Ref_Info_filename`=%s and `Item_filename`=%s'''
                if cursor.execute(dinctinct_sql, (item['Ref_Info_filename'], item['Item_filename'])):
                    return
            with conn.cursor() as cursor:
                insert_sql = '''insert into `Ref_Rel` (`Ref_Info_filename`,`Item_filename`,`download_ts`) values (%s,%s,%s);'''
                if cursor.execute(insert_sql, (
                        item['Ref_Info_filename'],
                        item['Item_filename'], item['download_ts']
                )):
                    conn.commit()
                    return item

    # 给没有id的单独哈希
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

    # 给没有id的单独哈希
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

    # 给 NULL 的FUND单独一个哈希
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

    # todo check item['whichtable']
    def insert_item(self, item):
        with create_conn() as conn:
            # 创建详情表
            whichtable = item['whichtable']
            with conn.cursor() as cursor:
                if not cursor.execute(f'show tables like "{whichtable}"'):
                    sql = f"""create table `{whichtable}`(
                        _id int auto_increment primary key,
                        dbname varchar(255),
                        filename varchar(255) not null unique,
                        pcode varchar(255),
                        pykm varchar(255),
                        year varchar(20),
                        issue varchar(20),
                        ISSN varchar (255),
                        dbcode varchar(255),
                        title longtext,
                        author_id longtext,
                        orgn_id longtext,
                        catalog_ABSTRACT longtext,
                        catalog_KEYWORD longtext,
                        catalog_ZCDOI varchar(255),
                        catalog_FUND longtext,
                        catalog_FUND_id longtext,
                        catalog_ZTCLS varchar(255),
                        download_ts varchar(255)
                    ) default charset=utf8;"""
                    cursor.execute(sql)
                    conn.commit()
            # 插入详情表
            with conn.cursor() as cursor:
                try:
                    select_item_sql = f'''select `filename` from `{whichtable}` where `filename`=%s'''
                    if cursor.execute(select_item_sql, item['filename']):
                        return
                    if item.get('easy_insert'):
                        insert_item_sql = f'''insert into `{whichtable}` (`filename`,`title`,`author_id`,`orgn_id`,`year`) values (%s,%s,%s,%s,%s)'''
                        cursor.execute(insert_item_sql, (
                            item['filename'], item['title'], item['author_id'], item['orgn_id'], item['year']
                        ))
                        conn.commit()
                        return item
                    else:
                        insert_item_sql = f'''insert into `{whichtable}` (`dbname`,`filename`,`pcode`,`pykm`,`year`,`issue`,`ISSN`,`dbcode`,`title`,`author_id`,`orgn_id`,`catalog_ABSTRACT`,`catalog_KEYWORD`,`catalog_ZCDOI`,`catalog_FUND`,`catalog_FUND_id`,`catalog_ZTCLS`,`download_ts`) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'''

                        cursor.execute(insert_item_sql, (
                            item['dbname'], item['filename'], item['pcode'], item['pykm'], item['year'],
                            item['issue'], item['ISSN'], item['dbcode'], item['title'], item['author_id'],
                            item['orgn_id'], item['catalog_ABSTRACT'], item['catalog_KEYWORD'],
                            item['catalog_ZCDOI'], item['catalog_FUND'], item['catalog_FUND_id'],
                            item['catalog_ZTCLS'], item['download_ts']
                        ))
                        conn.commit()
                        return item
                except Exception as e:
                    conn.rollback()
                    conn.commit()

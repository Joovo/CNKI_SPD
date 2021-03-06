from hashlib import md5
import time

dbname_lst = [
    {
        'dbname': '中国学术期刊网络出版总库', 'id': "CJFQ"
    },
    {
        'dbname': '中国博士学位论文全文数据库', 'id': "CDFD"
    },
    {
        'dbname': '中国优秀硕士学位论文全文数据库', 'id': "CMFD"
    },
    {
        'dbname': '中国图书全文数据库', 'id': "CBBD"
    },
    {
        'dbname': '国际期刊数据库', 'id': "SSJD"
    },
    {
        'dbname': '外文题录数据库', 'id': "CRLDENG"
    },
]


def get_md5():
    m = md5()
    time_str = str(int(time.time_ns()))
    m.update(time_str.encode())
    return m.hexdigest()

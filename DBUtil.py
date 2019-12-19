# -*- coding:utf-8 -*-
from DBUtils.PooledDB import PooledDB
from bin.globalval import POOL_CONFIG
import pymysql

repo_host = POOL_CONFIG['repo_ip']
repo_port = int(POOL_CONFIG['repo_port'])
repo_user = POOL_CONFIG['repo_user']
repo_passwd = POOL_CONFIG['repo_passwd']
repo_dbname = POOL_CONFIG['repo_dbname']


class SingletonPool():
    _instance_pool = []

    def __new__(cls, *args, **kwargs):
        for _args, _kwargs, _instance in cls._instance_pool:
            if (_args, _kwargs) == (args, kwargs):
                return _instance
        else:  # 单例池中无参数相同的单例
            _instance = super().__new__(cls)
            cls._instance_pool.append((args, kwargs, _instance))
            return _instance


class DBManager(SingletonPool):
    def __init__(self, host, port, user, passwd, dbname):
        connKwargs = {'host': host, 'port': port, 'user': user, 'passwd': passwd, 'db': dbname, 'charset': "utf8"}
        self._pool = PooledDB(creator=pymysql, mincached=2, maxcached=10, maxshared=10, maxusage=10000, **connKwargs)

    def getConn(self):
        return self._pool.connection()


_repoManager = DBManager(repo_host, repo_port, repo_user, repo_passwd, repo_dbname)

"""
功能说明：获取连接池中的连接
参数:

返回值:
    返回数据库连接

错误抛出:
"""


def getRepoConn():
    return _repoManager.getConn()


"""
功能说明：获取管理库中的一条信息
参数:
    sqltext : sql文件内容

返回值:
    返回结果元组

错误抛出:
"""


def queryOne(sql, conn=None):
    if conn is None:
        conn = getRepoConn()
    cursor = conn.cursor()
    rowcount = cursor.execute(sql)
    if rowcount > 0:
        res = cursor.fetchone()
    else:
        res = None
    cursor.close()
    conn.close()
    return res


"""
功能说明：获取管理库中的一条信息
参数:
    sqltext : sql文件内容

返回值:
    返回结果字典，key为字段名称

错误抛出:
"""


def queryOneDict(sql, conn=None):
    if conn is None:
        conn = getRepoConn()
    rest_dict = {}
    cursor = conn.cursor()
    rowcount = cursor.execute(sql)
    columnlist = [column_desc[0] for column_desc in cursor.description]
    if rowcount > 0:
        res = cursor.fetchone()
        for key, value in zip(columnlist, res):
            rest_dict[key] = value
    else:
        rest_dict = None
    cursor.close()
    conn.close()
    return rest_dict


"""
功能说明：获取管理库中的一条信息
参数:
    sqltext : sql文件内容

返回值:
    返回结果多条记录组成的元组

错误抛出:
"""


def queryAll(sql, conn=None):
    if conn is None:
        conn = getRepoConn()
    cursor = conn.cursor()
    rowcount = cursor.execute(sql)
    if rowcount > 0:
        res = cursor.fetchall()
    else:
        res = None
        rowcount = 0
    cursor.close()
    conn.close()

    return rowcount, res


"""
功能说明：获取管理库中的一条信息
参数:
    sqltext : sql文件内容

返回值:
    返回结果
    rowcount:结果条数
    res_dict:结果组成的字典，键为列名称，值为列数据组成的列表

错误抛出:
"""


def queryAllDict(sql, conn=None):
    if conn is None:
        conn = getRepoConn()
    res_dict = {}
    cursor = conn.cursor()
    rowcount = cursor.execute(sql)
    columnlist = [column_desc[0] for column_desc in cursor.description]
    if rowcount > 0:
        res = cursor.fetchall()
        for i in range(rowcount):
            for key, value in zip(columnlist, res[i]):
                res_dict.setdefault(key, []).append(value)
    else:
        res_dict = None
    cursor.close()
    conn.close()

    return rowcount, res_dict


"""
功能说明：执行sql语句
参数:
    sql   : sql文件内容
    param : 参数，一般是一个列表或者元组，用来替代sql中的问号

返回值:
    返回执行条数

错误抛出:
"""


def execute(sql, conn=None, param=None):
    if conn is None:
        conn = getRepoConn()
    cursor = conn.cursor()
    rowcount = 0

    try:
        if param == None:
            rowcount = cursor.execute(sql)
        else:
            rowcount = cursor.execute(sql, param)
    except:
        conn.rollback()
    conn.commit()
    cursor.close()
    conn.close()

    return rowcount


def execute_sqllist(sqllist, conn=None):
    if conn is None:
        conn = getRepoConn()
    cursor = conn.cursor()

    try:
        for sql in sqllist:
            cursor.execute(sql)
    except:
        conn.rollback()
        print("出现错误")
    conn.commit()
    cursor.close()
    conn.close()
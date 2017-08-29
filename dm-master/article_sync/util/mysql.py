# coding=utf-8
import logging
import urlparse
from random import choice
import MySQLdb
from DBUtils.PooledDB import PooledDB

from base.config import Config

logger = logging.getLogger(__name__)

MASTER_MYSQL_POOL = []
SLAVE_MYSQL_POOL = []
    
    
class InitMysqlConnectPool(object):
    def __init__(self):
        global MASTER_MYSQL_POOL
        global SLAVE_MYSQL_POOL
        if not MASTER_MYSQL_POOL:
            master_urls = Config["master.url"]
            logger.info("===master_urls: %s", master_urls)
            
            if master_urls:
                for url in master_urls.split(";"):
                    pool = InitMysqlConnectPool._init_mysql_pool(url, Config["master.mincached"], Config["master.maxcached"],
                                                Config["master.maxshared"], Config["master.maxconnections"])
                    MASTER_MYSQL_POOL.append(pool)
            logger.info("MASTER_MYSQL_POOL is: %s", MASTER_MYSQL_POOL)
                    
        if not SLAVE_MYSQL_POOL:
            slave_urls = Config["slave.url"]
            logger.info("===slave_urls: %s", slave_urls)
            if slave_urls:
                for url in slave_urls.split(";"):
                    pool = InitMysqlConnectPool._init_mysql_pool(url, Config["slave.mincached"], Config["slave.maxcached"],
                                                Config["slave.maxshared"], Config["slave.maxconnections"])
                    SLAVE_MYSQL_POOL.append(pool)
            logger.info("SLAVE_MYSQL_POOL is: %s", SLAVE_MYSQL_POOL)
            
        super(InitMysqlConnectPool, self).__init__()
    
    @staticmethod
    def _init_mysql_pool(jdbc_url, mincached=1, maxcached=4, maxshared=5, maxconnections=5):
        """
        :param jdbc_url: mysql://root:xxx@xiaoqiang-zdm:3306/mysql
        :return:
        """
        if not jdbc_url:
            return
    
        logger.info("jdbc_url: %s", jdbc_url)
        conf = urlparse.urlparse(jdbc_url)
        logger.info("hostname: %s, db: %s, user: %s, passwd: %s, port: %s",
                      conf.hostname, conf.path, conf.username, conf.password, conf.port)
    
        db = ''
        if len(conf.path) > 1:
            db = conf.path[1:]

        return PooledDB(MySQLdb, mincached=int(mincached), maxcached=int(maxcached),
                        maxshared=int(maxshared), maxconnections=int(maxconnections),
                        host=conf.hostname, port=int(conf.port), user=conf.username,
                        passwd=conf.password, db=db, charset='utf8')

    
class MysqlClient(InitMysqlConnectPool):
    def __init__(self, mode="slave"):
        super(MysqlClient, self).__init__()
        pool = None
        if "slave" == mode:
            pool = choice(SLAVE_MYSQL_POOL)
        elif "master" == mode:
            pool = choice(MASTER_MYSQL_POOL)

        self._conn = pool.connection()
        self._cursor = self._conn.cursor()
        
    def getAll(self, sql, param=None, dispose=True):
        """
        @summary: 执行查询，并取出所有结果集
        @param sql:查询ＳＱＬ，如果有查询条件，请只指定条件列表，并将条件值使用参数[param]传递进来
        @param param: 可选参数，条件列表值（元组/列表）
        @param dispose: 是否关闭cursor，conn连接
        @return: result list(字典对象)/boolean 查询到的结果集
        """
        if param is None:
            count = self._cursor.execute(sql)
        else:
            count = self._cursor.execute(sql, param)
        if count > 0:
            result = self._cursor.fetchall()
        else:
            result = False
        self.close(dispose)
        return result

    def getOne(self, sql, param=None, dispose=True):
        """
        @summary: 执行查询，并取出第一条
        @param sql:查询ＳＱＬ，如果有查询条件，请只指定条件列表，并将条件值使用参数[param]传递进来
        @param param: 可选参数，条件列表值（元组/列表）
        @param dispose: 是否关闭cursor，conn连接
        @return: result list/boolean 查询到的结果集
        """
        if param is None:
            count = self._cursor.execute(sql)
        else:
            count = self._cursor.execute(sql, param)
        if count > 0:
            result = self._cursor.fetchone()
        else:
            result = False
        self.close(dispose)
        return result
    
    def getMany(self, sql, num, param=None, dispose=True):
        """
        @summary: 执行查询，并取出num条结果
        @param sql:查询ＳＱＬ，如果有查询条件，请只指定条件列表，并将条件值使用参数[param]传递进来
        @param num:取得的结果条数
        @param param: 可选参数，条件列表值（元组/列表）
        @param dispose: 是否关闭cursor，conn连接
        @return: result list/boolean 查询到的结果集
        """
        if param is None:
            count = self._cursor.execute(sql)
        else:
            count = self._cursor.execute(sql, param)
        if count > 0:
            result = self._cursor.fetchmany(num)
        else:
            result = False
        self.close(dispose)
        return result

    def insertOne(self, sql, value, dispose=True):
        """
        @summary: 向数据表插入一条记录
        @param sql:要插入的ＳＱＬ格式
        @param value:要插入的记录数据tuple/list
        @param dispose: 是否关闭cursor，conn连接
        @return: insertId 受影响的行数
        """
        self._cursor.execute(sql, value)
        result = self.__getInsertId()
        self.close(dispose)
        return result
    
    def insertMany(self, sql, values, dispose=True):
        """
        @summary: 向数据表插入多条记录
        @param sql:要插入的ＳＱＬ格式
        @param values:要插入的记录数据tuple(tuple)/list[list]
        @param dispose: 是否关闭cursor，conn连接
        @return: count 受影响的行数
        """
        count = self._cursor.executemany(sql, values)
        self.end()
        self.close(dispose)
        return count
    
    def __query(self, sql, param=None):
        if param is None:
            count = self._cursor.execute(sql)
        else:
            count = self._cursor.execute(sql, param)
        return count
    
    def update(self, sql, param=None, autocommit=False, dispose=True):
        """
        @summary: 更新数据表记录
        @param sql: ＳＱＬ格式及条件，使用(%s,%s)
        @param param: 要更新的  值 tuple/list
        @param autocommit: 是否自动提交，默认手动提交
        @param dispose: 是否关闭cursor，conn连接
        @return: count 受影响的行数
        """
        if not autocommit:
            # self.begin()
            count = self.__query(sql, param)
            self.end()
        else:
            count = self.__query(sql, param)
        
        self.close(dispose)
        
        return count
    
    def delete(self, sql, param=None, autocommit=False, dispose=True):
        """
        @summary: 删除数据表记录
        @param sql: ＳＱＬ格式及条件，使用(%s,%s)
        @param param: 要删除的条件 值 tuple/list
        @param autocommit: 是否自动提交，默认手动提交
        @param dispose: 是否关闭cursor，conn连接
        @return: count 受影响的行数
        """
        if not autocommit:
            self.begin()
            count = self.__query(sql, param)
            self.end()
        else:
            count = self.__query(sql, param)
        
        self.close(dispose)
        return count
    
    def begin(self):
        """
        @summary: 开启事务
        """
        self._conn.autocommit(0)
    
    def end(self, option='commit'):
        """
        @summary: 结束事务
        """
        if option == 'commit':
            self._conn.commit()
        else:
            self._conn.rollback()
    
    def dispose(self, isEnd=1):
        """
        @summary: 释放连接池资源
        """
        if isEnd == 1:
            self.end('commit')
        else:
            self.end('rollback')
        self.close()
        
    def close(self, dispose=True):
        if dispose:
            self._cursor.close()
            self._conn.close()
        
    def __getInsertId(self):
        """
        获取当前连接最后一次插入操作生成的id,如果没有则为０
        """
        self._cursor.execute("SELECT @@IDENTITY AS id")
        result = self._cursor.fetchall()
        return result[0]['id']

# -*- coding: utf-8 -*-
import pymysql

class ApiError(Exception):
    pass

class MysqlConnect(object):
    """docstring for MysqlConnect"""
    def __init__(self, host, port, db_name, user, passwd, charset):
        self.host = host
        self.port = port
        self.db_name = db_name
        self.user = user
        self.passwd = passwd
        self.charset = charset
        self.open = True
        self.get_conn()

    def get_conn(self):
        self.conn = pymysql.connect(host=self.host, port=self.port, user=self.user, passwd=self.passwd, db=self.db_name, use_unicode=True, charset=self.charset, cursorclass=pymysql.cursors.DictCursor, connect_timeout=10, autocommit=True)

    def get_cursor(self):
        cursor = None
        try:
            if self.conn.open:
                cursor = self.conn.cursor()
            else:
                try:
                    self.get_conn()
                    cursor = self.conn.cursor()
                except Exception as e:
                    raise ApiError(-100, "数据库连接失败...")
        except Exception as e:
            try:
                self.conn.ping()
                cursor = self.conn.cursor()
            except Exception as e:
                raise ApiError(-100, "数据库连接失败...")
        return cursor

    def deal_sql(self, sql, param=None):
        try:
            with self.get_cursor() as cur:
                cur.execute(sql, param)
                res = cur.fetchall()
        except Exception as e:
            try:
                with self.get_cursor() as cur:
                    cur.execute(sql, param)
                    res = cur.fetchall()
            except Exception as e:
                raise ApiError(-100, e)
        return res

    def close(self):
        self.conn.close()
#--*-- coding:utf-8 --*--
import sqlite3
from contextlib import contextmanager

class Db(object):
    def __init__(self):
        self.db='test.db'
        self.conn=None

    def connect(self):
        try:
            self.conn=sqlite3.connect(self.db)
        except Exception as e:
            return str(e)
        return True

    @contextmanager
    def cursor(self):
        cursor=self.conn.cursor()
        yield cursor
        cursor.close()

    def execute(self,sql):
        if sql:
            with self.cursor() as cursor:
                ret=cursor.execute(sql)
                result=cursor.fetchall()
            return result

    def execute_commit(self,sql):
        pass







#--*-- coding:utf-8 --*--
from utils.dbconnect import *
import unittest
class TestDbConnector(unittest.TestCase):
    def setUp(self):
        self.db=Db()

    def test_db(self):
        ret=self.db.connect()
        print ret
        self.assertEqual(ret,True,'连接成功')
        create_sql='''create table user(
        user_id int primary key,
        name text,
        passwd int)'''
        sql='''select * from user'''
        ret=self.db.execute(sql)
        print ret
        self.assertEqual(ret,True,'query success')



if __name__ == '__main__':
    unittest.main()
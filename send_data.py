import pymysql
from DB_info import *


class MySQL:
    def __init__(self, _info):
        '''
        sql_eng = "mysql+mysqldb://%s:%s@%s:%s/%s" % (DBInfo['user'], DBInfo['password'], DBInfo['host'],
                                                      DBInfo['port'], DBInfo['db'])
        self.engine = create_engine(sql_eng, encoding=DBInfo['charset'])
        '''
        self.conn = pymysql.connect(host=_info['host'],
                                    port=_info['port'],
                                    user=_info['user'],
                                    password=_info['password'],
                                    db=_info['db'],
                                    charset=_info['charset'])
        self.cursor = self.conn.cursor(pymysql.cursors.DictCursor)

    def close(self):
        self.conn.close()

    def create_table(self, _table_name, _line):
        sql = f"SHOW TABLES LIKE '{_table_name}'"
        result = self.cursor.execute(sql)
        if not result:
            sql = f"create table {_table_name}{_line};"

            self.cursor.execute(sql)
            self.conn.commit()
        else:
            print("테이블 존재")


DB_info['db'] = 'dt_king'
mydb = MySQL(DB_info)
mydb.create_table("buy_list", "(id INT primary key AUTO_INCREMENT,Date datetime,Code char(7),OBJ Decimal,Price Decimal)")
mydb.close()

#Date date primary key,Open Decimal,High Decimal,Low Decimal,Close Decimal, Volume Decimal
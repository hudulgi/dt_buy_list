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
        _sql = f"SHOW TABLES LIKE '{_table_name}'"
        _result = self.cursor.execute(_sql)
        if not _result:
            sql = f"create table {_table_name}({_line});"

            self.cursor.execute(sql)
            self.conn.commit()
        else:
            print("테이블 존재")

    def insert_data(self, _table, _data):
        _col = list()
        _val = list()
        for _c, _v in _data.items():
            _col.append(_c)
            _val.append(_v)
        _col = ", ".join(_col)
        _val = ", ".join(_val)
        _sql = f"INSERT INTO {_table}({_col}) VALUES ({_val})"
        print(_sql)
        self.cursor.execute(_sql)
        self.conn.commit()


DB_info['db'] = 'dt_king'
mydb = MySQL(DB_info)
#mydb.create_table("buy_list22", "id INT primary key AUTO_INCREMENT,Date datetime,Code char(7),OBJ Decimal,Price Decimal")

#sql = "INSERT INTO buy_list(Date, Code, OBJ, Price) VALUES (%s, %s, %s, %s)"
#       INSERT INTO buy_list(Date, Code, OBJ, Price) VALUES (2021-02-18 09:38:00, A111111, 1, 2)
#mydb.cursor.execute(sql,                                  ("2021-02-18 08:38:00", "A123456", "111", "100"))
#mydb.conn.commit()
mydb.insert_data("buy_list", {"Date": "2021-02-18 00:00:00", "Code": "A123457", "OBJ": "111", "Price": "100"})
mydb.close()

#Date date primary key,Open Decimal,High Decimal,Low Decimal,Close Decimal, Volume Decimal
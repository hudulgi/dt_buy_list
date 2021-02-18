import pymysql
from DB_info import *
import pandas as pd
import datetime


class MySQL:
    def __init__(self, _info):
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
            _val.append('"' + _v + '"')
        _col = ", ".join(_col)
        _val = ", ".join(_val)
        _sql = f"INSERT INTO {_table}({_col}) VALUES ({_val})"
        print(_sql)
        self.cursor.execute(_sql)
        self.conn.commit()

    def data_duplicate_check(self, _date, _code, _table='buy_list'):
        _sql = f"SELECT Code FROM {_table} WHERE DATE(Date)='{_date}'"
        _df = pd.read_sql(_sql, con=self.conn)
        _list_from_df = _df.Code.tolist()
        if _code in _list_from_df:
            return True
        else:
            return False


class ManBuyList:
    def __init__(self, _path):
        self.data_path = _path

    def read_file(self, _file):
        return pd.read_csv(self.data_path + "\\" + _file)


if __name__ == "__main__":
    now = datetime.datetime.now()

    buy = ManBuyList("C:\\CloudStation\\dt_data\\daily_data\\buy_list")
    df = buy.read_file("buy_%s.csv" % now.strftime("%y%m%d"))

    mydb = MySQL(DB_info)
    #mydb.create_table("buy_list", "id INT primary key AUTO_INCREMENT,Date datetime,Code char(7),OBJ Decimal,Price Decimal")

    for i in df.index:
        val = df.at[i, 'Code']
        if not mydb.data_duplicate_check(now.strftime("%Y-%m-%d"), val):
            mydb.insert_data("buy_list", {"Date": df.at[i, 'Date'],
                                          "Code": df.at[i, 'Code'],
                                          "OBJ": str(df.at[i, 'Object']),
                                          "Price": str(df.at[i, 'Price'])})
        else:
            print("항목중복")
    mydb.close()

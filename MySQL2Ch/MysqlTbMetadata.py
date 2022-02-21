import pymysql

from StatementParser import MySqlTbCreateStmt


class MysqlTbMetadata(object):
    protocol_list = ["mysql", "clickhouse"]

    __db_host = "127.0.0.1"
    __db_port = "3306"
    __db_name = ""
    __db_user = ""
    __db_passwd = ""
    __db_charset = "utf8"
    __protocol = "mysql"
    __tb_name = ""
    __create_stmt = ""

    def __init__(self, host, port, name, user, passwd, protocol, tb_name):
        self.__db_host = host
        self.__db_port = port
        self.__db_name = name
        self.__db_user = user
        self.__db_passwd = passwd
        self.__protocol = protocol
        self.__tb_name = tb_name

    # 获取jdbc连接url
    def jdbc_url(self):
        return "jdbc:%s://%s:%s/%s" % (self.__protocol, self.__db_host,
                                       self.__db_port, self.__db_name)

    # 获取建表语句
    def tb_create_stmt(self):
        if self.__create_stmt == "":
            db = pymysql.connect(host=self.__db_host, user=self.__db_user,
                                 password=self.__db_passwd, database=self.__db_name,
                                 port=self.__db_port, charset='utf8')
            cursor = db.cursor()
            sql = 'show create table ' + self.__tb_name
            cursor.execute(sql)
            create_ddl = cursor.fetchall()
            raw_ddl = create_ddl[0][1]
            self.__create_stmt = MySqlTbCreateStmt(raw_ddl)
            cursor.close()
        return self.__create_stmt

    # 获取field列表
    def tb_column_list(self):
        self.tb_create_stmt()
        if self.__create_stmt == "":
            return []
        else:
            return self.__create_stmt.column_list()

    @staticmethod
    def column_list_str(column_list, protocol_type):
        res = []
        if protocol_type == "mysql":
            for item in column_list:
                if item.type_enum().is_number == 1:
                    column_str = "ifnull(`%s`,%s)" % (item.field_name(), item.default_exp())
                else:
                    column_str = "ifnull(`%s`,'%s')" % (item.field_name(), item.default_exp())
                res.append(column_str)

        elif protocol_type == "clickhouse":
            for item in column_list:
                res.append("`%s`" % item.field_name())
        return res

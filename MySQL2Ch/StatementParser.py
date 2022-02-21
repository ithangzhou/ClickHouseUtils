import re

# 解析mysql表ddl语句，生成对应的clickhouse-ddl语句
from MysqlDataType import MysqlDataType


class MySqlTbCreateStmt(object):
    __raw_statement = None
    __raw_tb_name = None
    __primary_key_col = None
    __field_list = None
    __auto_updated_col = ""

    def __init__(self, raw_ddl):
        self.__raw_statement = raw_ddl
        ddl_arr = raw_ddl.split("\n")
        ddl_len = len(ddl_arr)
        auto_update_col_list = []
        self.__field_list = []

        for i in range(ddl_len):
            # 顺序遍历ddl语句的每一行
            simple_stmt = ddl_arr[i].strip()

            j = simple_stmt.split(" ")
            col_len = len(j)
            if col_len > 2:
                column = j[0].replace("`", "").lower()
                i_type_full = j[1].lower()
                i_type = i_type_full.split("(")[0]

                # 获取表名
                if column == "create":
                    self.__raw_tb_name = j[2].replace("`", "").lower()
                # primary key列
                elif column == "primary":
                    self.__primary_key_col = MysqlKeyColumn(simple_stmt).key_field_list()

                # 获取列定义
                data_type = MysqlDataType.for_name(i_type)
                if data_type is not None:
                    field = MysqlField(simple_stmt)
                    self.__field_list.append(field)
                    if field.is_auto_updated() == 99:
                        self.__auto_updated_col = field.field_name()
                    elif field.is_auto_updated() > 0:
                        auto_update_col_list.append(field)

        if (self.__auto_updated_col == "") & (len(auto_update_col_list) > 0):
            self.__auto_updated_col = auto_update_col_list[0].field_name()

    def raw_statement(self):
        return self.__raw_statement

    def column_list(self):
        return self.__field_list

    def primary_key(self):
        return self.__primary_key_col

    def ch_ddl(self, ch_db_name,ch_tb_name):
        create_line = " create table if not exists %s.%s \non cluster bigdata_ck_cluster ( \n" % (ch_db_name,ch_tb_name)

        field_def_lines = ""
        for i in range(len(self.__field_list)):
            field_str = self.__field_list[i].ch_definition()
            delimeter = ",\n"
            if i == len(self.__field_list) - 1:
                delimeter = "\n"
            field_def_lines += field_str + delimeter

        # 表引擎定义，示例：
        # ) ENGINE = ReplacingMergeTree(modified_at) ORDER BY doctoruid SETTINGS index_granularity = 8192
        if self.__auto_updated_col == "":
            auto_update = ""
        else:
            auto_update = ", %s" % (self.__auto_updated_col)
        engine_line = " ) \nENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/%s/%s', '{shard}_{replica}'%s) \n ORDER BY (%s)  \nSETTINGS index_granularity = 8192\n" \
                      % (ch_db_name,ch_tb_name,auto_update, ",".join(self.__primary_key_col))
        return create_line + field_def_lines + engine_line


# 解析mysql表ddl语句中字段定义部分


class MysqlField(object):
    __raw_definition = ""
    __type_enum = None
    __full_type = ""
    # 对应的clickhouse值类型
    __unsigned = 0
    __comment = ""
    __ch_type = ""
    __auto_updated = 0
    __field_name = ""
    __default_exp = ""

    def __init__(self, raw_column_definition):
        self.__raw_definition = raw_column_definition.strip().lower()
        arr = self.__raw_definition.split(" ")
        self.__field_name = arr[0].strip("`")
        self.__full_type = arr[1].lower()
        i_type = self.__full_type.split("(")[0]
        data_type = MysqlDataType.for_name(i_type)
        self.__type_enum = data_type
        assert data_type is not None
        self.__ch_type = data_type.ch_type
        # 处理有符号的整型数据
        if (arr[2].lower() == "unsigned") & (self.__full_type.find('int') >= 0):
            self.__unsigned = 1
            self.__ch_type = "U" + data_type.ch_type
        # 提取定义的默认值
        default_val = data_type.default_val
        try:
            default_ind = arr.index("default")
            if arr[default_ind + 1] not in ("current_timestamp", "null"):
                default_val = arr[default_ind + 1].strip("'")
        except:
            pass
        self.__default_exp = default_val
        # 提取字段注释部分
        if self.__raw_definition.find(" comment ") > -1:
            self.__comment = self.__raw_definition.split(" comment ")[1].strip(" ',")
        else:
            self.__comment = ""
        # 参数化字段类型
        if i_type in ("decimal"):
            self.__ch_type = self.__full_type.replace(i_type, self.__ch_type)
        # 提取记录时间戳，用作版本号
        field_name = arr[0].strip("`")
        if field_name == "gmt_modified":
            self.__auto_updated = 2
        elif field_name == "modified_at":
            self.__auto_updated = 3
        if raw_column_definition.upper().find(" DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP ") > -1:
            self.__auto_updated = 99

    def is_auto_updated(self):
        return self.__auto_updated

    def comment(self):
        return self.__comment

    def ch_type(self):
        return self.__ch_type

    def ch_definition(self):
        return " `%s`  %s  comment '%s'" % (self.__field_name, self.__ch_type, self.__comment)

    def field_name(self):
        return self.__field_name

    def type_enum(self):
        return self.__type_enum

    def default_exp(self):
        return self.__default_exp


# 解析mysql表ddl语句中索引/主键定义部分
class MysqlKeyColumn(object):
    key_type_list = ["primary", "unique", "key"]
    __raw_definition = ""
    __key_type = ""
    # __name = ""
    __field_list = []
    reg_exp = re.compile(r'[(](.*?)[)]', re.S)

    def __init__(self, raw_key_definition):
        self.__raw_definition = raw_key_definition.strip().lower()
        res = re.findall(MysqlKeyColumn.reg_exp, self.__raw_definition)
        self.__field_list = res[0].replace("`", "").replace(" ", "").split(",")
        arr = self.__raw_definition.split("(")
        if arr[0].startswith("primary"):
            self.__key_type = self.key_type_list[0]
        elif arr[0].startswith("unique"):
            self.__key_type = self.key_type_list[1]
        elif arr[0].startswith("key"):
            self.__key_type = self.key_type_list[2]

    def key_field_list(self):
        return self.__field_list

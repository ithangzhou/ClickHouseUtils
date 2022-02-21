from enum import Enum


class MysqlDataType(Enum):
    # string类型
    varchar = "varchar", "String", 0, 1, ""
    char = "char", "String", 0, 1, ""
    tinyblob = "tinyblob", "String", 0, 1, ""
    tinytext = "tinytext", "String", 0, 1, ""
    blob = "blob", "String", 0, 1, ""
    mediumblob = "mediumblob", "String", 0, 1, ""
    mediumtext = "mediumtext", "String", 0, 1, ""
    longblob = "longblob", "String", 0, 1, ""
    longtext = "longtext", "String", 0, 1, ""
    text = "text", "String", 0, 1, ""

    # 日期时间类型
    timestamp = "timestamp", "DateTime", 1, 0, "toDateTime('1970-01-02 00:00:00')"
    datetime = "datetime", "DateTime",  1, 0, "toDateTime('1970-01-02 00:00:00')"
    date = "date", "Date",  1, 0, "toDate('1970-01-02')"
    time = "time", "Int32", 1, 0, "0"
    year = "year", "Int8", 1, 0, "0"

    # 数值类型
    bigint = "bigint", "Int64", 1, 0, "0"
    int = "int", "Int32", 1, 0, "0"
    integer = "integer", "Int32", 1, 0, "0"
    mediumint = "mediumint", "Int32", 1, 0, "0"
    smallint = "smallint", "Int16", 1, 0, "0"
    tinyint = "tinyint", "Int8", 1, 0, "0"
    float = "float", "Float32", 1, 0, "0.0"
    double = "double", "Float64", 1, 0, "0.0"
    decimal = "decimal", "Decimal", 1, 0, "0.0"

    def __init__(self, name, ch_type, is_number, is_string, default_val):
        self.__name = name
        self.__ch_type = ch_type
        self.__is_number = is_number
        self.__is_string = is_string
        self.__default_val = default_val

    @property
    def name(self):
        return self.__name

    @property
    def ch_type(self):
        return self.__ch_type

    @property
    def is_number(self):
        return self.__is_number

    @property
    def is_string(self):
        return self.__is_string

    @property
    def default_val(self):
        return self.__default_val

    @staticmethod
    def for_name(name):
        enums = list(MysqlDataType)
        for item in enums:
            if item.name == name:
                return item
        return

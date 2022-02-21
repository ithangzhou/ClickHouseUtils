import json
import time
import logging
import os
import sys

from clickhouse_driver import Client

"""
clickhouse库jwy_ods表初始化
-- 生成ddl语句
-- datax同步json脚本
-- 生成数据加载sql
"""

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
proj_path = rootPath + '/../../'

from MysqlTbMetadata import MysqlTbMetadata

# 参数解析
mysql_param = []
clickhouse_param = {}
with open(rootPath+"/../conf/mysql2ch_params.json", "r") as param_obj:
    params = json.load(param_obj)

for group_param in params:
    mysql_param = group_param["mysql"]
    clickhouse_param = group_param["clickhouse"]

    if len(mysql_param['table']) != len(clickhouse_param['table']):
        logging.error("源表数量和目标表数量不一致，程序终止")
        exit(-1)

    for i in range(len(mysql_param["table"])):
        src_tb_name = mysql_param["table"][i]
        tag_tb_name = clickhouse_param["table"][i]

        # 解析源表的元数据信息
        src_table = MysqlTbMetadata(mysql_param["db_host"], mysql_param["db_port"], mysql_param["db_name"],
                                    mysql_param["db_user"], mysql_param["db_passwd"], "mysql", src_tb_name)

        # print(src_table.tb_create_stmt().raw_statement())
        src_columns = src_table.tb_column_list()
        src_column_list = MysqlTbMetadata.column_list_str(src_columns, "mysql")
        target_column_list = MysqlTbMetadata.column_list_str(src_columns, "clickhouse")
        json_data = None

        # 生成clickhouse建表语句，并创建表
        clickhouse_create_stmt = src_table.tb_create_stmt().ch_ddl(clickhouse_param["db_name"],tag_tb_name)
        print( clickhouse_create_stmt +";\n")
        # 执行ddl建表语句
        exec_ddl=clickhouse_param["exec_ddl"]
        if exec_ddl:
            client = Client(host=clickhouse_param["db_host"], database=clickhouse_param["db_name"],
                            user=clickhouse_param["db_user"]
                            , password=clickhouse_param["db_passwd"])
            # client.execute(clickhouse_create_stmt)
        # 生成etl-sql语句
        sql_valid=clickhouse_param["sql_etl"]
        if sql_valid:
            prefix='insert into %s.%s \nselect \n' % (clickhouse_param["db_name"],tag_tb_name)
            suffix='\nfrom %s_copy.%s\n;' % (mysql_param["db_name"],src_tb_name)
            fields=', \n'.join(src_column_list)
            sql=prefix + fields + suffix 
            etl_sql=(proj_path + "out/etl_sql/%s_%s.sql") % (clickhouse_param["db_name"], tag_tb_name)
            with open(etl_sql,"w") as f:
                f.write(sql)

        # 生成datax配置
        datax_valid=clickhouse_param["datax_conf"]
        # 根据datax配置模板，设置同步参数
        if datax_valid:
            with open(rootPath + "/../conf/ch_datax_template.json", "r") as config:
                json_data = json.load(config)
                reader_param = json_data["job"]["content"][0]["reader"]["parameter"]
                reader_param["column"] = src_column_list
                reader_param["connection"][0]["jdbcUrl"] = ["jdbc:mysql://%s:%d/%s" % (mysql_param["db_host"],
                                                                                    mysql_param["db_port"],
                                                                                    mysql_param["db_name"])]
                reader_param["connection"][0]["table"] = ["%s" % (src_tb_name)]
                reader_param["password"] = mysql_param["db_passwd"]
                reader_param["username"] = mysql_param["db_user"]
                reader_param["splitPk"] = src_table.tb_create_stmt().primary_key()[0]

                writer_param = json_data["job"]["content"][0]["writer"]["parameter"]
                writer_param["column"] = target_column_list
                writer_param["connection"][0]["jdbcUrl"] = "jdbc:clickhouse://%s:%d/%s" % (clickhouse_param["db_host"],
                                                                                            clickhouse_param["db_port"],
                                                                                            clickhouse_param["db_name"])
                writer_param["connection"][0]["table"] = ["%s" % (tag_tb_name)]
                writer_param["password"] = clickhouse_param["db_passwd"]
                writer_param["username"] = clickhouse_param["db_user"]
                writer_param["table"] = tag_tb_name

            # 生成datax同步配置
            datax_json = (proj_path + "out/datax/%s_%s_%d.json") % (clickhouse_param["db_name"], tag_tb_name, time.time())
            with open(datax_json, "w") as f:
                json.dump(json_data, f)

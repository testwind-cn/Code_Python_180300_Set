#!/usr/bin/env python3
# _*_ coding:utf-8 _*_
# _*_ Author:WangJun _*_

# 利用sparksql计算输出th_branch_info_statictis表

import os
import sys

from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext

import datetime


class StrTool:

    @staticmethod
    def get_param_int(index, default=0):
        np = len(sys.argv)
        if index < np:
            the_str = sys.argv[index]
        else:
            the_str = ""
        if type(the_str) is str and the_str.isdigit():
            return int(the_str)
        else:
            return default

    @staticmethod
    def get_param_str(index, default=""):
        np = len(sys.argv)
        if index < np:
            return sys.argv[index]
        else:
            return default

    @staticmethod
    def get_the_date(the_day_str='', delta_day=0):
        """ # 19700501 to 21000101

        :param the_day_str: str
        :param delta_day: int
        :return: date
        """

        if the_day_str is None or type(the_day_str) is not str:
            the_day_str = ''
        try:
            sdate1 = datetime.datetime.strptime(the_day_str, "%Y%m%d").date()
            # stime = time.strptime(the_day_str, "%Y%m%d")
            sdate2 = sdate1 + datetime.timedelta(days=delta_day)
        except ValueError as e:
            sdate1 = datetime.date.today()
            sdate2 = sdate1 + datetime.timedelta(days=delta_day)
        return sdate2

    @staticmethod
    def get_the_date_str(the_day_str='', delta_day=0):
        sdate1 = StrTool.get_the_date(the_day_str=the_day_str, delta_day=delta_day)
        the_day_str = sdate1.strftime("%Y%m%d")
        #        curYear = 2018
        #        theday = theday.replace(curYear, theday.month, theday.day)
        #        the_day_str = str(theday).replace('-', '')
        return the_day_str


def add_branch_data(p_sql_context, p_the_date=""):
    # f_last_date = "20190122"
    # f_today_date = "20190123"
    f_today_date = StrTool.get_the_date_str(p_the_date)
    f_last_date = StrTool.get_the_date_str(f_today_date, -1)

    table_name = "th_branch_info_" + f_last_date

    print(f_today_date + ": " + table_name + " 查询上一日表数据")

    p_sql_s = """
select mcht_cd,branch_cd,appr_date,delete_date,aip_bran_cd,
  busi_area, account, account_name, branch_business_status 
from rds_posflow.""" + table_name
    
    b = p_sql_context.sql(p_sql_s)

    bk = b.map(lambda x: (x[0] + "-" + x[1], x))
    # >>> bk.count()  1774259

    print(f_today_date + ": rds_posflow.branch_apms_bl" + " 查询本日新增数据")

    p_sql_s = """
select mchtcd as mcht_cd,branchcd as branch_cd, 
  apprdate as appr_date,deletedate as delete_date,
  aipbrancd as aip_bran_cd, busiarea as busi_area, 
  account,accountname as account_name,
  branchbusinessstatus as branch_business_status 
from rds_posflow.branch_apms_bl
 where cast(file_date as INT) = """ + f_today_date

    a = p_sql_context.sql(p_sql_s)

    ak = a.map(lambda x: (x[0] + "-" + x[1], x))

    # -- >>> ak.count() 10491
    ak2 = ak.subtractByKey(bk)
    # -->>> ak2.count()  41
    ak3 = ak.subtractByKey(ak2)
    # -->>> ak3.count() 10450
    bk2 = bk.leftOuterJoin(ak2)
    bk3 = bk2.map(lambda x: (x[0], x[1][0] if x[1][1] is None else x[1][1]))
    bk4 = bk3.union(ak3)
    # -- >>> bk4.count() 1784709\\

    p_sql_context.sql("use rds_posflow")

    table_name = "th_branch_info_" + f_today_date
    table_name_tmp = table_name + "_tmp"

    df4 = bk4.values().toDF()
    df4.registerTempTable(table_name_tmp)

    print(f_today_date + ": " + table_name_tmp + " 临时表数据准备完毕")

    print(f_today_date + ": " + table_name + " 表数据插入开始")

    p_sql_s = """
CREATE TABLE if not exists  rds_posflow.{}
( mcht_branch_id	string,
  mcht_cd	string,
  branch_cd	string,
  appr_date	string,
  delete_date	string,
  aip_bran_cd	string,
  busi_area	string,
  account	string,
  account_name	string,
  branch_business_status	string
) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '"',
  'escapeChar' = '\\\\'
)
STORED AS TEXTFILE""".format(table_name)

    p_sql_context.sql(p_sql_s)    
    p_sql_context.sql("truncate table {}".format(table_name))


# " ( mcht_branch_id ,mcht_cd,branch_cd,appr_date,delete_date,aip_bran_cd,busi_area,account,branch_business_status ) " + \

    p_sql_s = """
insert into rds_posflow.{}
select concat( mcht_cd ,'-',branch_cd) as mcht_branch_id ,
mcht_cd,branch_cd, appr_date, delete_date,
aip_bran_cd, busi_area, account, account_name,
branch_business_status from {}
""".format(table_name, table_name_tmp)

    p_sql_context.sql(p_sql_s)

    print(f_today_date + ": " + table_name + " 表数据插入完成")


def cal_branch_info(p_sql_context, p_the_date=""):
    # f_last_date = "20190122"
    # f_today_date = "20190123"
    f_today_date = StrTool.get_the_date_str(p_the_date)
    table_name = "th_branch_info_" + f_today_date

    print(f_today_date + ": th_branch_info_statictis 表数据开始插入")

    p_sql_s = """
CREATE TABLE if not exists rds_posflow.th_branch_info_statictis
( mcht_cd	string,
  branch_num	string,
  max_busi_area	string,
  total_area	string,
  is_large_shopping_mcht	string,
  last_update_date	string,
  last_update_time	string
) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '"',
    'escapeChar' = '\\\\'
)
STORED AS TEXTFILE
"""
    p_sql_context.sql(p_sql_s)
    p_sql_context.sql("truncate table rds_posflow.th_branch_info_statictis")

    p_sql_s = """
insert into rds_posflow.th_branch_info_statictis 
select
    mcht_cd, count(*) as branch_num,
    max(if(length(regexp_extract(busi_area,'([^0-9]*)([0-9]*)([^0-9]*)',2) ) <= 0, 0, cast(regexp_extract(busi_area,'([^0-9]*)([0-9]*)([^0-9]*)',2) as BIGINT) )) as max_busi_area,
    sum(if(length(regexp_extract(busi_area,'([^0-9]*)([0-9]*)([^0-9]*)',2) ) <= 0, 0, cast(regexp_extract(busi_area,'([^0-9]*)([0-9]*)([^0-9]*)',2) as BIGINT) )) as total_area,
    if (max(length(account)) > 5, 'Y', 'N') as is_large_shopping_mcht,
    cast(current_date() as string)  as last_update_date,
    cast(current_timestamp() as string) as last_update_time
from rds_posflow.{} group by mcht_cd""".format(table_name)

    p_sql_context.sql(p_sql_s)

    print(f_today_date + ": th_branch_info_statictis 表数据已插入完成")


if __name__ == "__main__":

    f_project_id = StrTool.get_param_int(1, 1)
    f_day_str = StrTool.get_param_str(2, "")
    f_days = StrTool.get_param_int(3, 1)
    f_day_str = StrTool.get_the_date_str(f_day_str)

    # encoding=utf8
    reload(sys)
    sys.setdefaultencoding('utf8')

    os.environ['SPARK_HOME'] = "/opt/cloudera/parcels/CDH/lib/spark"
    sys.path.append("/opt/cloudera/parcels/CDH/lib/spark/python")

    conf = (SparkConf()
            .setMaster("yarn-client")
            .setAppName("th_branch_info_statictis: " + str(f_project_id)+" " + f_day_str + " " + str(f_days) )
            .set("spark.executor.memory", "6g"))
    sc = SparkContext(conf=conf)
    sqlContext = HiveContext(sc)

    if (f_project_id & 1) > 0:
        add_branch_data(sqlContext, f_day_str)

    if (f_project_id & 2) > 0:
        cal_branch_info(sqlContext, f_day_str)

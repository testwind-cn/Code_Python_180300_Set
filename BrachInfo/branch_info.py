#!/usr/bin/env python3
# _*_ coding:utf-8 _*_
# _*_ Author:WangJun _*_

# 利用sparksql计算输出th_branch_info_statictis表
# spark-submit  /hdfs/branch_info.py 1 20190204

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
            print(e)
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
    f_this_date = StrTool.get_the_date_str(p_the_date)
    f_last_date = StrTool.get_the_date_str(f_this_date, -1)

    table_last_name = "th_branch_info_" + f_last_date
    table_this_name = "th_branch_info_" + f_this_date

    p_sql_context.sql("use rds_posflow")

    print(f_this_date + ": " + table_this_name + " 表数据插入开始")

    # p_sql_s = "DROP TABLE if exists rds_posflow.{}".format(table_this_name)
    p_sql_s = "ALTER TABLE rds_posflow.th_branch_info DROP IF EXISTS PARTITION (cal_date='{}')".format(f_this_date)
    p_sql_context.sql(p_sql_s)

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
) CLUSTERED BY (mcht_cd) into 20 BUCKETS
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '"',
  'escapeChar' = '\\\\'
)
STORED AS TEXTFILE""".format(table_this_name)

    # p_sql_context.sql(p_sql_s)

    # p_sql_context.sql("truncate table rds_posflow.{}".format(table_this_name))

    p_sql_context.sql("set hive.enforce.bucketing = true")
    p_sql_context.sql("set mapreduce.job.reduces=20")
    # 上面一个没有起作用，下面一个有用
    p_sql_context.sql("set mapred.reduce.tasks = 20")

    p_sql_s = """
INSERT OVERWRITE TABLE rds_posflow.th_branch_info PARTITION (cal_date='{}')
    select 
      if( s.mcht_cd is null and s.branch_cd is null, d.mcht_branch_id, s.mcht_branch_id ) as mcht_branch_id,
      if( s.mcht_cd is null and s.branch_cd is null, d.mcht_cd, s.mcht_cd ) as mcht_cd,
      if( s.mcht_cd is null and s.branch_cd is null, d.branch_cd, s.branch_cd ) as branch_cd,
      if( s.mcht_cd is null and s.branch_cd is null, d.appr_date, s.appr_date ) as appr_date,
      if( s.mcht_cd is null and s.branch_cd is null, d.delete_date, s.delete_date ) as delete_date,
      if( s.mcht_cd is null and s.branch_cd is null, d.aip_bran_cd, s.aip_bran_cd ) as aip_bran_cd,
      if( s.mcht_cd is null and s.branch_cd is null, d.busi_area, s.busi_area ) as busi_area,
      if( s.mcht_cd is null and s.branch_cd is null, d.account, s.account ) as account,
      if( s.mcht_cd is null and s.branch_cd is null, d.account_name, s.account_name ) as account_name,
      if( s.mcht_cd is null and s.branch_cd is null, d.branch_business_status, s.branch_business_status ) as branch_business_status
    from (
      select
        concat( mchtcd ,'-',branchcd) as mcht_branch_id ,
        mchtcd as mcht_cd,
        branchcd as branch_cd,
        apprdate as appr_date,
        deletedate as delete_date,
        aipbrancd as aip_bran_cd,
        busiarea as busi_area,
        account,accountname as account_name,
        branchbusinessstatus as branch_business_status
      from rds_posflow.branch_apms_bl
      where
        cast(file_date as INT) = {} CLUSTER by( mcht_cd)
         )
      s full outer join ( 
        select
         concat( mcht_cd ,'-',branch_cd) as mcht_branch_id ,
         mcht_cd,branch_cd, appr_date, delete_date,
         aip_bran_cd, busi_area, account, account_name,
         branch_business_status
        from rds_posflow.th_branch_info where cal_date='{}'
        CLUSTER by( mcht_cd)
        )
      d on s.mcht_branch_id = d.mcht_branch_id 
      CLUSTER by( mcht_cd)
    """.format(f_this_date, f_this_date, f_last_date)

    p_sql_context.sql(p_sql_s)

    print(f_this_date + ": " + "th_branch_info (cal_date='{}')".format(f_this_date) + " 表数据插入完成")


def cal_branch_info(p_sql_context, p_the_date=""):
    # f_last_date = "20190122"
    # f_today_date = "20190123"
    f_today_date = StrTool.get_the_date_str(p_the_date)
    table_name = "th_branch_info_" + f_today_date

    print(f_today_date + ": th_branch_info_statictis 表数据开始插入")

    p_sql_s = "DROP TABLE if exists rds_posflow.th_branch_info_statictis"
    p_sql_context.sql(p_sql_s)

    p_sql_s = """
CREATE TABLE if not exists rds_posflow.th_branch_info_statictis
( mcht_cd	string,
  branch_num	string,
  max_busi_area	string,
  total_area	string,
  is_large_shopping_mcht	string,
  last_update_date	string,
  last_update_time	string
) CLUSTERED BY (mcht_cd) into 20 BUCKETS
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '"',
    'escapeChar' = '\\\\'
)
STORED AS TEXTFILE
"""
    p_sql_context.sql(p_sql_s)
    p_sql_context.sql("truncate table rds_posflow.th_branch_info_statictis")

    p_sql_context.sql("set hive.enforce.bucketing = true")
    p_sql_context.sql("set mapreduce.job.reduces=20")
    # 上面一个没有起作用，下面一个有用
    p_sql_context.sql("set mapred.reduce.tasks = 20")

    p_sql_s = """
insert into rds_posflow.th_branch_info_statictis 
select
    mcht_cd, count(*) as branch_num,
    max(if(length(regexp_extract(busi_area,'([^0-9]*)([0-9]*)([^0-9]*)',2) ) <= 0, 0, cast(regexp_extract(busi_area,'([^0-9]*)([0-9]*)([^0-9]*)',2) as BIGINT) )) as max_busi_area,
    sum(if(length(regexp_extract(busi_area,'([^0-9]*)([0-9]*)([^0-9]*)',2) ) <= 0, 0, cast(regexp_extract(busi_area,'([^0-9]*)([0-9]*)([^0-9]*)',2) as BIGINT) )) as total_area,
    if(max(length(account)) > 5, 'Y', 'N') as is_large_shopping_mcht,
    cast(current_date() as string)  as last_update_date,
    cast(current_timestamp() as string) as last_update_time
from rds_posflow.th_branch_info where cal_date='{}' group by mcht_cd CLUSTER by( mcht_cd)""".format(f_today_date)

    print(p_sql_s)

    p_sql_context.sql(p_sql_s)

    print(f_today_date + ": th_branch_info_statictis 表数据已插入完成")


if __name__ == "__main__":

    f_project_id = StrTool.get_param_int(1, 3)
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

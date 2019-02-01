#!/usr/bin/env python3
# _*_ coding:utf-8 _*_
# _*_ Author:Peter.Zou _*_

#利用sparksql计算输出merchant_to_lerong表
import os
import sys

# encoding=utf8
reload(sys)
sys.setdefaultencoding('utf8')

os.environ['SPARK_HOME'] = "/opt/cloudera/parcels/CDH/lib/spark"
sys.path.append("/opt/cloudera/parcels/CDH/lib/spark/python")

from pyspark import SparkConf,SparkContext
from pyspark.sql import HiveContext

conf =(SparkConf()
       .setMaster("yarn-client")
       .setAppName("merchant_branch")
       .set("spark.executor.memory", "6g"))
sc = SparkContext(conf = conf)
sqlContext = HiveContext(sc)

#清空数据
sqlContext.sql("truncate table dm_unify.merchant_to_lerong_spark")

insertData = '''
INSERT INTO dm_unify.merchant_to_lerong_spark
select
    merchant.merchant_ap as merchant_ap,
    NULL as type,
    (CASE WHEN kkd.apply_status='7' THEN datediff(CURRENT_DATE (),kkd.apply_status_change_time) ELSE 'NULL' END) AS loan_decline_days,
    (CASE WHEN kkd.apply_status in('11','15') THEN kkd.review_amount ELSE 'NULL' END) AS last_loan_amt,
    (CASE WHEN kkd.apply_status in('11','15') THEN kkd.apply_time ELSE 'NULL' END) AS last_app_date,
	(CASE WHEN kkd.apply_status in('11','15') THEN kkd.review_payment_type ELSE 'NULL' END) AS last_repay_type,
	(CASE WHEN kkd.apply_status in('11','15') THEN kkd.review_rate ELSE 'NULL' END) AS last_rate,
	(CASE WHEN kkd.apply_status in('11','15') THEN kkd.review_duration ELSE 'NULL' END) AS last_duration,
	merchant.merchant_type as mcht_type,
	merchant.approve_date as mcht_in_date,
	(CASE WHEN kkd.apply_status in('5','10') THEN 'Y' ELSE 'N' END) AS loanClient
from 
(
select DISTINCT mcht_tid from rds_rc.gb_loan_application_kkd where apply_status in('5','10','7','11')
) aa
left join 
(
SELECT
			*, Row_Number () OVER (
				PARTITION BY mcht_tid
				ORDER BY
					apply_status_change_time DESC
			) rank
		FROM
			rds_rc.gb_loan_application_kkd ) kkd on aa.mcht_tid=kkd.mcht_tid and kkd.rank=1
left join 
rds_rc.merchant merchant ON aa.mcht_tid = merchant.merchant_bl
'''

sqlContext.sql(insertData)

print("merchant_to_lerong表数据已插入完成")
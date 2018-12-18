# --coding=utf-8--
import csv
import StringIO

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import HiveContext
conf = SparkConf().setAppName('test')
sc = SparkContext(conf=conf)
sqlContext = HiveContext(sc)
fpath = "/home/testFolder/logflow/bl_shouyinbao/UTF8/20181101/9999100000/t1_trxrecord_20181101_V2.csv"
fpath = "/shouyinbao/t1_trxrecord_20181101_V2.csv"
tfile = sc.textFile(fpath)
rdd = tfile.map(your_method).map(lambda x : Row(**x))
#your_method 是自己定义的map函数
df = rdd.toDF()
#转化成DataFrame #存入Hive表中，mode有append, overwrite, error, ignore 这4种模式
df.write.saveAsTable('testtable',mode='mode有append')
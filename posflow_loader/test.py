#!/usr/bin/python3
# -*- coding: UTF-8 -*-

import os
import shutil
import pathlib
# import zipfile
import subprocess
# import platform
import datetime
# hdfs
from hdfs.client import Client
# hdfs
# hive
from impala.dbapi import connect
from impala.util import as_pandas
from wj_tools.file_check import MyLocalFile
from wj_tools.file_check import MyHdfsFile
from wj_tools.hdfsclient import MyClient  # hdfs
# hive
# data path config file
from conf import ConfigData
from wj_tools.str_tool import StrTool
from wj_tools import sftp_tool


def run_hdfs_test(conf: ConfigData):
    # the_date = conf.test_date()  # "20181101"
    client = Client(conf.hdfs_ip())  # "http://10.2.201.197:50070"
    # root_path = conf.unzip_dir(is_baoli)     # 'D:/DATA/UNZIP/'
    # dest_dir = conf.hdfs_dir_syb(is_baoli)

    # file_pre = conf.file_pre1()  # "t1_trxrecord_"
    # file_ext = conf.file_ext2()  # "_V2.csv"

    #    client.upload('/shouyinbao/', "/home/testFolder/logflow/bl_shouyinbao/UTF8/20181101/9999100000/t1_trxrecord_20181101_V2.csv", cleanup=True)
    dat = client.list('/', status=False)
    print(dat)


def run_hive_test(conf: ConfigData):
    host = conf.hive_ip()  # '10.2.201.197'
    port = conf.hive_port()  # 10000
    user = conf.hive_user()  # "hdfs"
    auth = conf.hive_auth()  # 'PLAIN'
    test = conf.hive_test()  # "select * from test.test1"

    conn = connect(host=host, port=port, auth_mechanism=auth, user=user, password='Redhat@2016')
    cur = conn.cursor()

    cur.execute(test)
    data = as_pandas(cur)
    print(data)

    cur.close()
    conn.close()


if __name__ == "__main__":

    a = sftp_tool.Sftp_Tool( h = '172.31.130.14', p = 22, u = 'root', s = 'Redhat@2016', r = '/ftpdata/thblposloan/posflow_loader/', d = 'C:\\Users\\wangjun\\Desktop\\python\\posflow_loader')
    a.openSFTP()
    ll = a.getRmFilesList('/ftpdata/thblposloan/posflow/')
    ll = a.getLcFilesList('C:\\Users\\wangjun\\Desktop\\python\\posflow_loader')
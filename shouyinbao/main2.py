#!coding:utf-8
# import sys
import os
import shutil
import pathlib
# import zipfile
# import subprocess
# import platform
# hdfs
from hdfs.client import Client
# hdfs
# hive
from impala.dbapi import connect
from impala.util import as_pandas
from wj_tools import sftp_tool
from wj_tools.file_check import myLocalFile
from wj_tools.file_check import MyHdfsFile
from wj_tools.hdfsclient import MyClient  # hdfs
# hive
# data path config file
from conf import ConfigData as cF
from wj_tools.str_tool import StrTool

def run_unzip_file(conf,the_date):
    the_date = StrTool.get_the_date_str(the_date)
    if (type(the_date) is str) and len(the_date) == 8:
        m_month = the_date[0:6]
        m_day = the_date[6:8]
    else:
        return

    root_path = conf.get_data("allinpay_data_zc")
    destdir = conf.get_data("allinpay_utf8_zc")

    #    ifile = '/home/testFolder/logflow/bl_shouyinbao/20181101/9999100000/t1_trxrecord_20181101_V2.csv'
    #    ofile = '/home/testFolder/logflow/bl_shouyinbao/UTF8/20181101/9999100000/t1_trxrecord_20181101_V2.csv'
    filepre = conf.file_pre1()  # "t1_trxrecord_"
    fileext = conf.file_ext1()  # "_v2.zip"

    print("Start\n")

    # os.path.join(root_path, the_date) # real SYB folder don't have date folder

    a_file = os.path.join(root_path, the_date+".zip")
    if myLocalFile.check_file(a_file, start="", ext=""):
        myLocalFile.unzip_the_file(a_file, destdir, fstr=the_date)

    a_file = os.path.join(root_path, the_date+"_agt.zip")
    if myLocalFile.check_file(a_file, start="", ext=""):
        myLocalFile.unzip_the_file(a_file, destdir, fstr=the_date)


def run_sftp_file(conf, the_date):
    the_date = StrTool.get_the_date_str(the_date)
    if (type(the_date) is str) and len(the_date) == 8:
        m_month = the_date[0:6]
        m_day = the_date[6:8]
    else:
        return

    a = sftp_tool.Sftp_Tool(h=conf.get_data("allinpay_ftp_ip_zc"), p=int(conf.get_data("allinpay_ftp_port_zc")),
                            u=conf.get_data("allinpay_ftp_user_zc"), s=conf.get_data("allinpay_ftp_pass_zc"),
                            r=conf.get_data("allinpay_ftp_folder_zc"),d=conf.get_data("allinpay_data_zc"))
    a.openSFTP()
    a.download_files(from_dir=cf.get_data("allinpay_ftp_folder_zc"),
                     to_dir=cf.get_data("allinpay_data_zc"), fstr=the_date)


if __name__ == "__main__":
    cf = cF(True)

    if cf.is_test():
        the_date1 = cf.test_date()
    else:
        the_date1 = StrTool.get_param_str(1, "")
        # days = StrTool.get_param_int(2, 1)

    run_sftp_file(cf, the_date1)
    run_unzip_file(cf, the_date1)

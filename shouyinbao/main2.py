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
from wj_tools.file_check import myHdfsFile
from wj_tools.hdfsclient import MyClient  # hdfs
# hive
# data path config file
from conf import ConfigData as cF
from wj_tools import datestr


def run_unzip_file(the_date):
    the_date = datestr.getTheDateStr(the_date)
    if (type(the_date) is str) and len(the_date) == 8:
        m_month = the_date[0:6]
        m_day = the_date[6:8]
    else:
        return

    root_path = cF.get_data("allinpay_data_zc")
    destdir = cF.get_data("allinpay_utf8_zc")


    #    ifile = '/home/testFolder/logflow/bl_shouyinbao/20181101/9999100000/t1_trxrecord_20181101_V2.csv'
    #    ofile = '/home/testFolder/logflow/bl_shouyinbao/UTF8/20181101/9999100000/t1_trxrecord_20181101_V2.csv'
    filepre = cF.file_pre1()  # "t1_trxrecord_"
    fileext = cF.file_ext1()  # "_v2.zip"

    print("Start\n")

    # os.path.join(root_path, the_date) # real SYB folder don't have date folder

    aFile = os.path.join(root_path, the_date+".zip")
    if myLocalFile.checkfile(aFile, start="", ext=""):
        myLocalFile.unzipTheFile(aFile, destdir, fstr=the_date)

    aFile = os.path.join(root_path, the_date+"_agt.zip")
    if myLocalFile.checkfile(aFile, start="", ext=""):
        myLocalFile.unzipTheFile(aFile, destdir, fstr=the_date)


def run_sftp_file(the_date):
    the_date = datestr.getTheDateStr(the_date)
    if (type(the_date) is str) and len(the_date) == 8:
        m_month = the_date[0:6]
        m_day = the_date[6:8]
    else:
        return

    a = sftp_tool.Sftp_Tool(h=cF.get_data("allinpay_ftp_ip_zc"), p=int(cF.get_data("allinpay_ftp_port_zc")),
                            u=cF.get_data("allinpay_ftp_user_zc"), s=cF.get_data("allinpay_ftp_pass_zc"),
                            r=cF.get_data("allinpay_ftp_folder_zc"),d=cF.get_data("allinpay_data_zc"))
    a.openSFTP()
    a.download_files(from_dir=cF.get_data("allinpay_ftp_folder_zc"),
                     to_dir=cF.get_data("allinpay_data_zc"), fstr=the_date)


if __name__ == "__main__":
    run_sftp_file("20181101")
    run_unzip_file("20181101")

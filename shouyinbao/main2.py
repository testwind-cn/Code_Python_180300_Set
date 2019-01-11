#!coding:utf-8

import os
import shutil
import pathlib
import datetime
from hdfs.client import Client
from impala.dbapi import connect
from wj_tools import sftp_tool
from wj_tools.file_check import MyLocalFile
from wj_tools.file_check import MyHdfsFile
from wj_tools.hdfsclient import MyClient  # hdfs
# hive
# data path config file
from conf import ConfigData
from wj_tools.str_tool import StrTool


def run_unzip_file(conf: ConfigData, the_date: str):
    the_date = StrTool.get_the_date_str(the_date)
    if (type(the_date) is str) and len(the_date) == 8:
        m_month = the_date[0:6]
        m_day = the_date[6:8]
    else:
        return

    root_path = conf.get_data("allinpay_data_zc")
    destdir = conf.get_data("allinpay_data_zc")

    #    ifile = '/home/testFolder/logflow/bl_shouyinbao/20181101/9999100000/t1_trxrecord_20181101_V2.csv'
    #    ofile = '/home/testFolder/logflow/bl_shouyinbao/UTF8/20181101/9999100000/t1_trxrecord_20181101_V2.csv'

    print("Start\n")

    # os.path.join(root_path, the_date) # real SYB folder don't have date folder

    f_name = conf.get_zip_name(the_date, 3)  # the_date+".zip"
    a_file = os.path.join(root_path, f_name)
    if MyLocalFile.check_file(a_file):
        MyLocalFile.unzip_the_file(a_file, destdir, p_name=the_date+"*")

    f_name = conf.get_zip_name(the_date,5)   # the_date+"_agt.zip"
    a_file = os.path.join(root_path, f_name)
    if MyLocalFile.check_file(a_file):
        MyLocalFile.unzip_the_file(a_file, destdir,  p_name=the_date+"*")


def run_sftp_file(conf: ConfigData, the_date: str):
    the_date = StrTool.get_the_date_str(the_date)
    if (type(the_date) is str) and len(the_date) == 8:
        m_month = the_date[0:6]
        m_day = the_date[6:8]
    else:
        return

    a = sftp_tool.Sftp_Tool(h=conf.get_data("allinpay_ftp_ip_zc"), p=int(conf.get_data("allinpay_ftp_port_zc")),
                            u=conf.get_data("allinpay_ftp_user_zc"), s=conf.get_data("allinpay_ftp_pass_zc"),
                            r=conf.get_data("allinpay_ftp_folder_zc"), d=conf.get_data("allinpay_data_zc"))
    a.openSFTP()
    a.download_files(from_dir=conf.get_data("allinpay_ftp_folder_zc"),
                     to_dir=conf.get_data("allinpay_data_zc"), p_name=the_date+"*.zip")


def run_conv_file_local_to_hdfs(conf: ConfigData, the_date: str):
    """

    :param conf:
    :param the_date:
    :return:
    """
    the_date = StrTool.get_the_date_str(the_date)
    client = MyClient(conf.hdfs_ip())  # "http://10.2.201.197:50070"
    root_path = conf.get_data("allinpay_data_zc")
    dest_dir1 = conf.get_data("allinpay_utf8_zc")
    dest_dir2 = conf.get_data("hdfs_dir_zc")
    file_ext3 = conf.get_data("file_ext3")  # _loginfo_rsp.txt          # 20181101_loginfo_rsp.txt
    file_ext4 = conf.get_data("file_ext4")  # _loginfo_rsp_agt.txt      # 20181101_loginfo_rsp_agt.txt
    file_ext5 = conf.get_data("file_ext5")  # _rxinfo_rsp.txt           # 20181101_rxinfo_rsp.txt
    file_ext6 = conf.get_data("file_ext6")  # _rxinfo_rsp_agt.txt      # 20181101_rxinfo_rsp_agt.txt

    print("Start\n")

    files = MyLocalFile.get_child_file(root_path)
    for aFile in files:
        short_name = os.path.basename(aFile).lower()
        if short_name == (the_date + file_ext3).lower() or \
                short_name == (the_date + file_ext4).lower() or \
                short_name == (the_date + file_ext5).lower() or \
                short_name == (the_date + file_ext6).lower():
            to_file1 = str(pathlib.PurePath(dest_dir1).joinpath(pathlib.PurePath(aFile).name))
            to_file2 = str(pathlib.PurePosixPath(dest_dir2).joinpath(pathlib.PurePath(aFile).name))
            MyLocalFile.conv_file_local(aFile, to_file1, need_first_line=True)
            MyHdfsFile.safe_make_dir(client, to_file2)
            # client.newupload(to_file2, to_file1, encoding='utf-8')
            the_file = client.status(to_file2, strict=False)
            if the_file is None:
                client.upload(to_file2, to_file1)
                client.set_permission(to_file2, 777)
            # client.set_owner(thePath,owner='hdfs',group='supergroup')
            elif the_file['type'].lower() == 'file':  # 'directory'
                client.set_permission(to_file2, 777)


def run_hive(conf: ConfigData, the_date: str):
    client = Client(conf.hdfs_ip())  # "http://10.2.201.197:50070"
    conn = connect(host=conf.hive_ip(), port=conf.hive_port(), auth_mechanism=conf.hive_auth(), user=conf.hive_user())
    cur = conn.cursor()

    the_date = StrTool.get_the_date_str(the_date)  # "20181101"
    root_path = conf.get_data("hdfs_dir_zc")  # "/data/posflow/allinpay_utf8_zc/"
    file_ext3 = conf.get_data("file_ext3")  # _loginfo_rsp.txt          # 20181101_loginfo_rsp.txt
    file_ext4 = conf.get_data("file_ext4")  # _loginfo_rsp_agt.txt      # 20181101_loginfo_rsp_agt.txt
    file_ext5 = conf.get_data("file_ext5")  # _rxinfo_rsp.txt           # 20181101_rxinfo_rsp.txt
    file_ext6 = conf.get_data("file_ext6")  # _rxinfo_rsp_agt.txt       # 20181101_rxinfo_rsp_agt.txt

    print("Start\n")

    file3 = str(pathlib.PurePosixPath(root_path).joinpath(the_date + file_ext3))
    file4 = str(pathlib.PurePosixPath(root_path).joinpath(the_date + file_ext4))
    file5 = str(pathlib.PurePosixPath(root_path).joinpath(the_date + file_ext5))
    file6 = str(pathlib.PurePosixPath(root_path).joinpath(the_date + file_ext6))

    f_list = [file3,file4,file5,file6]
    t_list = ["hive_table3", "hive_table4", "hive_table5", "hive_table6"]

    for n in range(0,4):
        if MyHdfsFile.isfile(client, f_list[n]):
            sql = 'LOAD DATA INPATH \'' + f_list[n] + '\' INTO TABLE ' + conf.get_data(t_list[n])  # 'test.t1_trxrecprd_v2_zc'
            # '\' OVERWRITE INTO TABLE test.t1_trxrecprd_v2_bl2'
            print("OK" + "  " + sql+"\n")
            cur.execute(sql)  # , async=True)

    cur.close()
    conn.close()


def run_remove_files(conf: ConfigData, the_date: str, delta_day=0):
    sdate = StrTool.get_the_date_str(the_date, delta_day)  # "20181101"
    data_path = os.path.join(conf.get_data_path(1), sdate)
    utf8_path = os.path.join(conf.get_utf8_path(1), sdate)
    hdfs_path = str(pathlib.PurePosixPath(conf.get_hdfs_path()).joinpath(sdate))
    shutil.rmtree(data_path, ignore_errors=True)
    shutil.rmtree(utf8_path, ignore_errors=True)
    client = MyClient(conf.hdfs_ip())  # "http://10.2.201.197:50070"
    client.delete(hdfs_path, recursive=True)
    # "/user/hive/warehouse/posflow.db/t1_trxrecprd_v2/t1_trxrecord_20181204_V2*.csv"
    # hive_table="posflow.t1_trxrecprd_v2",
    # file_pre1 = 't1_trxrecord_',
    # file_ext2 = "_V2.csv",

if __name__ == "__main__":
    the_conf = ConfigData(p_is_test=False)

    client = Client(the_conf.hdfs_ip())  # "http://10.2.201.197:50070"
    a = MyHdfsFile.get_child(client, "/data/posflow/allinpay_utf8_zc")
    b = MyHdfsFile.get_child_file(client,"/data/posflow/allinpay_utf8_zc")
    c = MyHdfsFile.get_child_dir(client, "/data/posflow/allinpay_utf8_zc")

    # test
    # MyHdfsFile.delete(client, "/data/posflow/allinpay_utf8_zc", "*agt_cpy*")
    # test

    if the_conf.is_test():
        day_str = the_conf.test_date()
        days = 9
    else:
        day_str = StrTool.get_param_str(1, "")
        days = StrTool.get_param_int(2, 1)

    day_str = StrTool.get_the_date_str(day_str)

    date1 = StrTool.get_the_date(day_str)
    for i in range(0, days):
        delta = days - i - 1
        date2 = date1 - datetime.timedelta(days=delta)
        day_str2 = date2.strftime("%Y%m%d")
        run_sftp_file(the_conf, day_str2)
        run_unzip_file(the_conf, day_str2)
        run_conv_file_local_to_hdfs(the_conf, day_str2)
        run_hive(the_conf, the_date=day_str2)
        run_remove_files(the_conf, day_str2, 0)

    print("ok")

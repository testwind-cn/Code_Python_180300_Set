#!coding:utf-8
import os
import shutil
import pathlib
# import zipfile
# import subprocess
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


def run_unzip_file(conf: ConfigData, the_date, folder_type=2, is_baoli=True):
    the_date = StrTool.get_the_date_str(the_date)
    if (type(the_date) is str) and len(the_date) == 8:
        m_month = the_date[0:6]
        m_day = the_date[6:8]
    else:
        return

    root_path = conf.root_path(is_baoli)
    dest_dir = conf.unzip_dir(is_baoli)

    file_pre = conf.file_pre1()  # "t1_trxrecord_"
    file_ext = conf.file_ext1()  # "_v2.zip"

    print("Start\n")

    # os.path.join(root_path, the_date) # real SYB folder don't have date folder
    branches = MyLocalFile.get_child(root_path)
    for aBranch in branches:
        if MyLocalFile.check_branch(aBranch):
            months = MyLocalFile.get_child(aBranch)
            for aMonth in months:
                the_month = MyLocalFile.check_month(aMonth)
                if the_month > 0 and "{:0>6d}".format(the_month) == m_month:
                    day_list = MyLocalFile.get_child(aMonth)
                    for aDay in day_list:
                        the_day = MyLocalFile.check_day(aDay)
                        if the_day > 0 and "{:0>2d}".format(the_day) == m_day:
                            files = MyLocalFile.get_child(aDay)
                            for aFile in files:
                                if MyLocalFile.check_file(aFile, start=file_pre, ext=file_ext):
                                    short_name = os.path.basename(aBranch)
                                    if folder_type == 1:
                                        new_path = os.path.join(dest_dir, short_name, m_month, m_day)
                                        # "{:0>6d}".format(month)  "{:0>2d}".format(day)
                                    else:
                                        new_path = os.path.join(dest_dir, m_month + m_day, short_name)
                                        # "{:0>6d}{:0>2d}".format(month, day)
                                    MyLocalFile.unzip_the_file(aFile, new_path)


def run_conv_file_local(conf: ConfigData, the_date: str, is_baoli=True):
    the_date = StrTool.get_the_date_str(the_date)
    root_path = conf.unzip_dir(is_baoli)
    dest_dir = conf.decode_dir(is_baoli)

    file_pre = conf.file_pre1()  # "t1_trxrecord_"
    file_ext = conf.file_ext2()  # "_V2.csv"

    print("Start\n")

    branches = MyLocalFile.get_child(os.path.join(root_path, the_date))
    for aBranch in branches:
        if MyLocalFile.check_branch(aBranch):
            files = MyLocalFile.get_child(aBranch)
            for aFile in files:
                if MyLocalFile.check_file(aFile, file_pre + the_date, file_ext):
                    MyLocalFile.conv_file_local(aFile, os.path.join(dest_dir, the_date, os.path.basename(aBranch), file_pre + the_date + file_ext), True)


def run_conv_file_hdfs(conf: ConfigData, the_date: str, is_baoli=True):
    the_date = StrTool.get_the_date_str(the_date)
    client = Client(conf.hdfs_ip())  # "http://10.2.201.197:50070"
    root_path = conf.unzip_dir(is_baoli)  # 'D:/DATA/UNZIP/'
    dest_dir = conf.hdfs_dir_syb(is_baoli)

    file_pre = conf.file_pre1()  # "t1_trxrecord_"
    file_ext = conf.file_ext2()  # "_V2.csv"

    print("Start\n")

    branches = MyLocalFile.get_child(os.path.join(root_path, the_date))
    for aBranch in branches:
        if MyLocalFile.check_branch(aBranch):
            files = MyLocalFile.get_child(aBranch)
            for aFile in files:
                if MyLocalFile.check_file(aFile, file_pre + the_date, file_ext):
                    MyHdfsFile.conv_file_hdfs(aFile,
                                              os.path.join(dest_dir,
                                                           the_date,
                                                           os.path.basename(aBranch),
                                                           file_pre + the_date + file_ext),
                                              client)


def run_conv_file_local_to_hdfs(conf: ConfigData, the_date: str, is_baoli=True):
    """

    # client.upload('/shouyinbao/', "/home/testFolder/logflow/bl_shouyinbao/UTF8/20181101/9999100000/t1_trxrecord_20181101_V2.csv", cleanup=True)
    # dat = client.list('/shouyinbao/', status=False)
    # print(dat)

    # root_path = "/home/bd/桌面/201811_flow/zc_shouyinbao/UNZIP/"
    # dest_dir1 = "/home/bd/桌面/201811_flow/zc_shouyinbao/UTF8/"
    # dest_dir2 = "/shouyinbao/zc_shouyinbao/UTF8/"

    # root_path = "/home/testFolder/logflow/bl_shouyinbao/UNZIP/"
    # dest_dir1 = "/home/testFolder/logflow/bl_shouyinbao/UTF8/"
    # dest_dir2 = "/shouyinbao/zc_shouyinbao/UTF8/"

    # i_file = '/home/testFolder/logflow/bl_shouyinbao/20181101/9999100000/t1_trxrecord_20181101_V2.csv'
    # o_file = '/home/testFolder/logflow/bl_shouyinbao/UTF8/20181101/9999100000/t1_trxrecord_20181101_V2.csv'

    :param conf:
    :param the_date:
    :param is_baoli:
    :return:
    """
    the_date = StrTool.get_the_date_str(the_date)
    client = MyClient(conf.hdfs_ip())  # "http://10.2.201.197:50070"
    root_path = conf.unzip_dir(is_baoli)
    dest_dir1 = conf.decode_dir(is_baoli)
    dest_dir2 = conf.hdfs_dir_syb(is_baoli)
    file_pre = conf.file_pre1()  # "t1_trxrecord_"
    file_ext = conf.file_ext2()  # "_V2.csv"

    print("Start\n")

    branches = MyLocalFile.get_child(os.path.join(root_path, the_date))
    for aBranch in branches:
        if MyLocalFile.check_branch(aBranch):
            files = MyLocalFile.get_child_file(aBranch)
            for aFile in files:
                if MyLocalFile.check_file(aFile, file_pre + the_date, file_ext):
                    to_file1 = os.path.join(dest_dir1, the_date, os.path.basename(aBranch), file_pre + the_date + file_ext)
                    to_file2 = str(pathlib.PurePosixPath(dest_dir2).joinpath(the_date, os.path.basename(aBranch), file_pre + the_date + file_ext))
                    MyLocalFile.conv_file_local(aFile, to_file1, need_first_line=False)
                    MyHdfsFile.safe_make_dir(client, to_file2)
                    # client.newupload(to_file2, to_file1, encoding='utf-8')
                    the_file = client.status(to_file2, strict=False)
                    if the_file is None:
                        client.upload(to_file2, to_file1)
                        client.set_permission(to_file2, 777)
                    # client.set_owner(thePath,owner='hdfs',group='supergroup')
                    elif the_file['type'].lower() == 'file':  # 'directory'
                        client.set_permission(to_file2, 777)


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


def run_remove_files(conf: ConfigData, the_date: str, delta_day=0):
    sdate = StrTool.get_the_date_str(the_date, delta_day)  # "20181101"
    zip_path = os.path.join(conf.unzip_dir(), sdate)
    utf8_path = os.path.join(conf.decode_dir(), sdate)
    hdfs_path = str(pathlib.PurePosixPath(conf.hdfs_dir_syb()).joinpath(sdate))
    shutil.rmtree(zip_path, ignore_errors=True)
    shutil.rmtree(utf8_path, ignore_errors=True)
    client = MyClient(conf.hdfs_ip())  # "http://10.2.201.197:50070"
    client.delete(hdfs_path, recursive=True)
    # "/user/hive/warehouse/posflow.db/t1_trxrecprd_v2/t1_trxrecord_20181204_V2*.csv"
    # hive_table="posflow.t1_trxrecprd_v2",
    # file_pre1 = 't1_trxrecord_',
    # file_ext2 = "_V2.csv",

    del_path = "/user/hive/warehouse/" + \
               conf.get_data("hive_table").replace('.', '.db/') + "/"
    del_file = conf.get_data("file_pre1") + sdate + conf.get_data("file_ext2").replace('.', '*.')
    MyHdfsFile.delete(client, del_path, del_file)


def run_hive(conf: ConfigData, the_date: str, is_baoli=True):
    client = Client(conf.hdfs_ip())  # "http://10.2.201.197:50070"
    conn = connect(host=conf.hive_ip(), port=conf.hive_port(), auth_mechanism=conf.hive_auth(), user=conf.hive_user())
    cur = conn.cursor()

    the_date = StrTool.get_the_date_str(the_date)  # "20181101"
    root_path = conf.hdfs_dir_syb(is_baoli)  # "/shouyinbao/bl_shouyinbao/UTF8/"
    file_pre = conf.file_pre1()  # "t1_trxrecord_"
    file_ext = conf.file_ext2()  # "_V2.csv"

    print("Start\n")

    idn = 0
    branches = MyHdfsFile.get_child(client, root_path + the_date)
    for aBranch in branches:
        if MyHdfsFile.check_branch(client, aBranch):
            files = MyHdfsFile.get_child(client, aBranch)
            for aFile in files:
                if MyHdfsFile.check_file(client, aFile, file_pre + the_date, file_ext):
                    # '/shouyinbao/bl_shouyinbao/UTF8/20181101/9999997900/t1_trxrecord_20181101_V2.csv'
                    to_file2 = str(pathlib.PurePosixPath(root_path).joinpath(the_date, os.path.basename(aBranch), file_pre + the_date + file_ext))
                    sql = 'LOAD DATA INPATH \'' + to_file2 + '\' INTO TABLE ' + conf.hive_table(is_baoli)  # 'test.t1_trxrecprd_v2_zc'
                    # '\' OVERWRITE INTO TABLE test.t1_trxrecprd_v2_bl2'
                    idn += 1
                    print(str(idn) + "  " + sql + "\n")
                    cur.execute(sql)  # , async=True)

    cur.close()
    conn.close()


# SELECT count(*) from t1_trxrecprd_v2_bl; 	5199590 # bad coding ,deleted
# SELECT count(*) from t1_trxrecprd_v2_bl2; 		99793  # one day
# select count(*) from test.t1_trxrecprd_V2_bl2; 5199590


if __name__ == "__main__":

    #    return_code = subprocess.call("./ftpcmd.sh", shell=True)
    #    print(return_code)

    the_conf = ConfigData(is_test=False)

    if the_conf.is_test():
        day_str = the_conf.test_date()
        days = 190
    else:
        day_str = StrTool.get_param_str(1, "")
        days = StrTool.get_param_int(2, 1)

    # test
    day_str = "20181202"
    run_remove_files(the_conf, day_str, 0)
    # test

    run_hdfs_test(the_conf)
    run_hive_test(the_conf)

    day_str = StrTool.get_the_date_str(day_str)

    del_range = 30  # 删除旧数据的时间范围，天
    keep_range = 7  # 保留最近旧数据的时间范围，天

    for i in range(0, del_range):
        run_remove_files(the_conf, day_str, -(days + keep_range + del_range - 1 - i))

    date1 = StrTool.get_the_date(day_str)
    for i in range(0, days):
        delta = days - i - 1
        date2 = date1 - datetime.timedelta(days=delta)
        day_str2 = date2.strftime("%Y%m%d")
        run_remove_files(the_conf, day_str2, - keep_range)
        run_remove_files(the_conf, day_str2, 0)
        run_unzip_file(the_conf, the_date=day_str2)
        run_conv_file_local_to_hdfs(the_conf, the_date=day_str2, is_baoli=False)
        run_hive(the_conf, the_date=day_str2, is_baoli=False)

    print("ok")

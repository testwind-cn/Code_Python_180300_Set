#!coding:utf-8
import os
import shutil
import pathlib
# import zipfile
import subprocess
# import platform
import datetime
# hdfs
from hdfs.client import Client
from hdfs.client import InsecureClient
# hdfs
# hive
from impala.dbapi import connect
from impala.util import as_pandas
from py_tools.file_check import MyLocalFile
from py_tools.file_check import MyHdfsFile
from py_tools.hdfsclient import MyClient  # hdfs
# hive
# data path config file
from conf_data import ConfigData
from py_tools.str_tool import StrTool


def run_unzip_file(configData: ConfigData, folder_type=2):
    f_date_str = configData.get_f_date()  # "20181101"

    if (type(f_date_str) is str) and len(f_date_str) == 8:
        m_month = f_date_str[0:6]
        m_day = f_date_str[6:8]
    else:
        return

    zip_path = configData.get_zip_path()
    data_path = configData.get_data_path()

    f_name = configData.get_zip_name("")  # "t1_trxrecord_" the_date # "_V2.csv"

    print("Start\n")

    # os.path.join(root_path, the_date) # real SYB folder don't have date folder
    branches = MyLocalFile.get_child_dir(zip_path)
    for aBranch in branches:
        if MyLocalFile.check_branch(aBranch):
            months = MyLocalFile.get_child_dir(aBranch)
            for aMonth in months:
                the_month = MyLocalFile.check_month(aMonth)
                if the_month > 0 and "{:0>6d}".format(the_month) == m_month:
                    day_list = MyLocalFile.get_child_dir(aMonth)
                    for aDay in day_list:
                        the_day = MyLocalFile.check_day(aDay)
                        if the_day > 0 and "{:0>2d}".format(the_day) == m_day:
                            files = MyLocalFile.get_child_file(aDay)
                            for aFile in files:
                                if MyLocalFile.check_file(aFile, p_name=f_name):
                                    short_name = os.path.basename(aBranch)
                                    if folder_type == 1:
                                        new_path = os.path.join(data_path, m_month, m_day, short_name)
                                        # "{:0>6d}".format(month)  "{:0>2d}".format(day)
                                    else:
                                        new_path = os.path.join(data_path, f_date_str, short_name)
                                        # "{:0>6d}{:0>2d}".format(month, day)
                                    p_name = configData.get_file_name(f_date_str)
                                    MyLocalFile.unzip_the_file(aFile, new_path, p_name)


def run_conv_file_local(configData: ConfigData):
    f_date_str = configData.get_f_date()  # "20181101"

    root_path = configData.get_data_path()
    dest_dir = configData.get_utf8_path()

    f_name = configData.get_file_name(f_date_str)  # "t1_trxrecord_" the_date # "_V2.csv"

    print("Start\n")

    branches = MyLocalFile.get_child(os.path.join(root_path, f_date_str))
    for aBranch in branches:
        if MyLocalFile.check_branch(aBranch):
            files = MyLocalFile.get_child(aBranch)
            for aFile in files:
                if MyLocalFile.check_file(aFile, f_name):
                    MyLocalFile.conv_file_local(aFile, os.path.join(dest_dir, f_date_str, os.path.basename(aBranch), f_name), True)


def run_conv_file_hdfs(configData: ConfigData):
    f_date_str = configData.get_f_date()  # "20181101"

    client = InsecureClient(configData.hdfs_ip(), user="admin")  # "http://10.2.201.197:50070"
    root_path = configData.get_data_path()  # 'D:/DATA/UNZIP/'
    dest_dir = configData.get_hdfs_path()

    f_name = configData.get_file_name(f_date_str)  # "t1_trxrecord_" the_date # "_V2.csv"

    print("Start\n")

    branches = MyLocalFile.get_child(os.path.join(root_path, f_date_str))
    for aBranch in branches:
        if MyLocalFile.check_branch(aBranch):
            files = MyLocalFile.get_child(aBranch)
            for aFile in files:
                if MyLocalFile.check_file(aFile, f_name):
                    MyHdfsFile.conv_file_hdfs(aFile,
                                              os.path.join(dest_dir,
                                                           f_date_str,
                                                           os.path.basename(aBranch),
                                                           f_name),
                                              client)


def run_conv_file_local_to_hdfs(configData: ConfigData):
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

    :param configData:
    :param the_date:
    :param is_baoli:
    :return:
    """
    f_date_str = configData.get_f_date()  # "20181101"

    a_client = InsecureClient(configData.hdfs_ip(), user="admin")   # "http://10.2.201.197:50070"
    # webhdfs 默认是 dr.who ，不能伪装成其他用户，可以在配置里修改 hadoop.http.staticuser.user=dr.who
    # https://www.cnblogs.com/peizhe123/p/5540845.html
    root_path = os.path.join(configData.get_data_path(), f_date_str)
    dest_dir1 = os.path.join(configData.get_utf8_path(), f_date_str)
    dest_dir2 = str(pathlib.PurePosixPath(configData.get_hdfs_path()).joinpath(f_date_str))

    print("Start\n")

    f_name = configData.get_file_name(f_date_str)  # "t1_trxrecord_" the_date # "_V2.csv"

    branches = MyLocalFile.get_child_dir(root_path)
    for aBranch in branches:
        if MyLocalFile.check_branch(aBranch):
            files = MyLocalFile.get_child_file(aBranch)
            f_a_branch = os.path.basename(aBranch)
            for aFile in files:
                if MyLocalFile.check_file(aFile, f_name):
                    to_file1 = str(pathlib.PurePath(dest_dir1).joinpath(f_a_branch, f_name))
                    to_file2 = str(pathlib.PurePosixPath(dest_dir2).joinpath(f_a_branch, f_name))
                    f_add_head = configData.get_hive_add_date(f_a_branch)
                    f_add_end = configData.get_hive_add_date("789")
                    f_need_head = not configData.get_hive_head()  # False
                    MyLocalFile.conv_file_local(aFile, to_file1, need_first_line=f_need_head,p_add_head=f_add_head, p_add_tail=f_add_end,quoting="")
                    MyHdfsFile.safe_make_dir(a_client, to_file2)
                    # client.newupload(to_file2, to_file1, encoding='utf-8')
                    the_file = a_client.status(to_file2, strict=False)
                    if the_file is None:
                        a_client.upload(to_file2, to_file1) #, encoding='utf-8')
                        a_client.set_permission(to_file2, 777)
                    # a_client.set_owner(thePath,owner='hdfs',group='supergroup')
                    elif the_file['type'].lower() == 'file':  # 'directory'
                        a_client.set_permission(to_file2, 777)


def run_remove_files(configData: ConfigData):
    f_date_str = configData.get_f_date()  # StrTool.get_the_date_str(the_date, delta_day)  # "20181101"
    data_path = os.path.join(configData.get_data_path(), f_date_str)
    utf8_path = os.path.join(configData.get_utf8_path(), f_date_str)
    hdfs_path = str(pathlib.PurePosixPath(configData.get_hdfs_path()).joinpath(f_date_str))

    a_client = InsecureClient(configData.hdfs_ip(), user="admin")  # "http://10.2.201.197:50070"

    shutil.rmtree(data_path, ignore_errors=True)
    shutil.rmtree(utf8_path, ignore_errors=True)
    try:
        a_client.delete(hdfs_path, recursive=True)
    except:
        pass


def run_remove_hive(configData: ConfigData):
    f_date_str = configData.get_f_date()  # "20181101"
    p_date_str = configData.get_p_date()  # "2018-11-01"
    # "/user/hive/warehouse/rds_posflow.db/t1_trxrecprd_v2/t1_trxrecord_20181204_V2*.csv"

    del_table = configData.get_table_name()   # hive_table="rds_posflow.t1_trxrecprd_v2"

    if not configData.get_has_partition():
        del_file = configData.get_file_name(f_date_str).replace('.', '*.')
        MyHdfsFile.delete_hive_ssh(configData.cdh_ip(), table=del_table, p_name=del_file, username=configData.cdh_user(), password=configData.cdh_pass())

    else:
        conn = connect(host=configData.hive_ip(), port=configData.hive_port(), auth_mechanism=configData.hive_auth(), user=configData.hive_user())
        cur = conn.cursor()

        # "ALTER TABLE rds_posflow.t1_trxrecprd_v2_tmp DROP IF EXISTS PARTITION(p_date=2019-02-08) "
        sql = "ALTER TABLE {} DROP IF EXISTS PARTITION( p_date='{}' )".format(del_table, p_date_str)
        print(sql)
        cur.execute(sql)

        cur.close()
        conn.close()


def run_hive(configData: ConfigData):
    a_client = InsecureClient(url=configData.hdfs_ip(), user="admin")  # "http://10.2.201.197:50070"
    conn = connect(host=configData.hive_ip(), port=configData.hive_port(), auth_mechanism=configData.hive_auth(), user=configData.hive_user())
    cur = conn.cursor()

    f_date_str = configData.get_f_date()  # "20181101"
    p_date_str = configData.get_p_date()  # "2018-11-01"

    root_path = configData.get_hdfs_path()  # "/shouyinbao/bl_shouyinbao/UTF8/"
    file_name = configData.get_file_name(f_date_str)  # "t1_trxrecord_" the_date # "_V2.csv"
    table_name = configData.get_table_name()

    print("Start\n")

    idn = 0
    branches = MyHdfsFile.get_child(a_client, str(pathlib.PurePosixPath(root_path).joinpath(f_date_str)))
    for aBranch in branches:
        if MyHdfsFile.check_branch(a_client, aBranch):
            files = MyHdfsFile.get_child(a_client, aBranch)
            f_a_branch = MyHdfsFile.get_name(aBranch)
            for aFile in files:
                if MyHdfsFile.check_file(a_client, aFile, file_name):
                    # '/shouyinbao/bl_shouyinbao/UTF8/20181101/9999997900/t1_trxrecord_20181101_V2.csv'
                    to_file2 = str(pathlib.PurePosixPath(root_path).joinpath(f_date_str, f_a_branch, file_name))
                    if not configData.get_has_partition():
                        sql = "LOAD DATA INPATH '{}' INTO TABLE {}".format(to_file2, table_name)  # 'test.t1_trxrecprd_v2_zc'
                    # '\' OVERWRITE INTO TABLE test.t1_trxrecprd_v2_bl2'
                    else:
                        sql = "LOAD DATA INPATH '{}' INTO TABLE {} PARTITION ( p_date='{}' )".format(to_file2, table_name, p_date_str)  # 'test.t1_trxrecprd_v2_zc'
                    idn += 1
                    print(str(idn) + "  " + sql + "\n")
                    cur.execute(sql)  # , async=True)

    cur.close()
    conn.close()


if __name__ == "__main__":
    # http://10.91.1.21:50070/webhdfs/v1/Project?op=LISTSTATUS&user.name=hdfs
    m_is_test = False

    m_project_id = StrTool.get_param_int(1, 2)
    start_date_str = StrTool.get_the_date_str(StrTool.get_param_str(2, ""))
    m_days = StrTool.get_param_int(3, 1)


    if m_is_test:
        m_project_id = 2
        start_date_str = "20180901"
        m_days = 9  # 190

    start_date = StrTool.get_the_date(start_date_str)
    the_conf = ConfigData(m_project_id, StrTool.get_the_date_str_by_date(start_date, 0, 10), p_is_test=m_is_test)

    if m_project_id == 1:
        return_info = subprocess.run("/app/code/posflow_loader/ftpcmd.sh", shell=True)
        print(return_info.returncode)

    f_delta = the_conf.get_file_date_delta()
    # start_date_str = StrTool.get_the_date_str(start_date_str, - int(f_delta))

    del_range = 30  # 删除旧数据的时间范围，天
    keep_range = 7  # 保留最近旧数据的时间范围，天

    for i in range(0, del_range):
        delta = m_days + keep_range + del_range - 1 - i
        date2 = start_date - datetime.timedelta(days=delta)
        m_day_str3 = date2.strftime("%Y-%m-%d")
        the_conf.find_row(m_day_str3)
        run_remove_files(the_conf)

    for i in range(0, m_days):
        delta = m_days - i - 1
        date2 = start_date - datetime.timedelta(days=delta)
        date3 = start_date - datetime.timedelta(days=(delta + keep_range))

        the_conf.find_row(date3.strftime("%Y-%m-%d"))
        run_remove_files(the_conf)

        the_conf.find_row(date2.strftime("%Y-%m-%d"))
        run_remove_files(the_conf)

        run_remove_hive(the_conf)

        run_unzip_file(the_conf)

        run_conv_file_local_to_hdfs(the_conf)
        run_hive(the_conf)

    print("ok")

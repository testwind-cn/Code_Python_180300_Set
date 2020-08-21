#!coding:utf-8

import os
import shutil
import pathlib
import datetime
from hdfs.client import Client
from hdfs.client import InsecureClient
from impala.dbapi import connect
from py_tools import sftp_tool
from py_tools.file_check import MyLocalFile
from py_tools.file_check import MyHdfsFile
from py_tools.hdfsclient import MyClient  # hdfs
# hive
# data path config file
from conf_data import ConfigData
from py_tools.str_tool import StrTool


def run_sftp_file(configData: ConfigData):
    f_date_str = configData.get_f_date()  # "20181101"

    # allinpay_ftp_folder_bl_1 or allinpay_ftp_folder_bl_2
    f_dir = configData.get_remote_path_ftp(f_date_str)
    # allinpay_data_bl
    t_dir = os.path.join(configData.get_local_path_ftp(), f_date_str)
    # "file_ext" + str(configData.the_id)
    file_name = configData.get_ftp_name(f_date_str)

    a = sftp_tool.Sftp_Tool(h=configData.get_ftp_ip(), p=int(configData.get_ftp_port()),
                            u=configData.get_ftp_user(), s=configData.get_ftp_pass(),
                            r=f_dir, d=t_dir)
    a.openSFTP()
    a.download_files(from_dir=f_dir, to_dir=t_dir, p_name=file_name)


def run_conv_file_local_to_hdfs(configData: ConfigData):
    """

    :param configData:
    :return:
    """
    f_date_str = configData.get_f_date()  # "20181101"
    a_client = InsecureClient(configData.hdfs_ip(), user="admin")  # "http://10.2.201.197:50070"

    root_path = os.path.join(configData.get_data_path(), f_date_str)                        # allinpay_data_bl
    dest_dir1 = os.path.join(configData.get_utf8_path(), f_date_str)                        # allinpay_utf8_bl
    dest_dir2 = str(pathlib.PurePosixPath(configData.get_hdfs_path()).joinpath(f_date_str)) # hdfs_dir_bl
    # file_ext7 = configData.get_data("file_ext7")  # _loginfo_rsp_bl_new.csv   # 20181101_loginfo_rsp_bl_new.csv
    # file_ext8 = configData.get_data("file_ext8")  # _rsp_agt_bl_new.del       # 20181101_rsp_agt_bl_new.del
    # file_ext9 = configData.get_data("file_ext9")  # _rxinfo_rsp_bl.txt        # 20181101_rxinfo_rsp_bl.txt

    # f_list = [file_ext7, file_ext8, file_ext9]

    print("Start\n")

    # "file_ext" + str(configData.the_id)
    file_name = configData.get_file_name(f_date_str).lower()

    files = MyLocalFile.get_child_file(root_path)
    for aFile in files:
        short_name = os.path.basename(aFile).lower()
        f_name = pathlib.PurePath(aFile).name
        if short_name == file_name:
            to_file1 = str(pathlib.PurePath(dest_dir1).joinpath(f_name))
            to_file2 = str(pathlib.PurePosixPath(dest_dir2).joinpath(f_name))
            f_add_date = configData.get_hive_add_date(f_date_str)
            f_need_head = not configData.get_hive_head()
            MyLocalFile.conv_file_local(aFile, to_file1, need_first_line=f_need_head, p_add_head=f_add_date)
            MyHdfsFile.safe_make_dir(a_client, to_file2)
            # a_client.newupload(to_file2, to_file1, encoding='utf-8')
            the_file = a_client.status(to_file2, strict=False)
            if the_file is None:
                a_client.upload(to_file2, to_file1)
                a_client.set_permission(to_file2, 777)
            # a_client.set_owner(thePath,owner='hdfs',group='supergroup')
            elif the_file['type'].lower() == 'file':  # 'directory'
                a_client.set_permission(to_file2, 777)


def run_hive(configData: ConfigData):
    a_client = InsecureClient(url=configData.hdfs_ip(), user="admin")  # "http://10.2.201.197:50070"
    conn = connect(host=configData.hive_ip(), port=configData.hive_port(), auth_mechanism=configData.hive_auth(), user=configData.hive_user())
    cur = conn.cursor()

    f_date_str = configData.get_f_date()  # "20181101"
    p_date_str = configData.get_p_date()  # "2018-11-01"

    # hdfs_dir_bl
    root_path = str(pathlib.PurePosixPath(configData.get_hdfs_path()).joinpath(f_date_str))
    file_name = str(pathlib.PurePosixPath(root_path).joinpath(configData.get_file_name(f_date_str)))
    # "/data/posflow/allinpay_utf8_zc/20181101/"
    # 20181101_loginfo_rsp_bl_new.csv
    # 20181101_rsp_agt_bl_new.del
    # 20181101_rxinfo_rsp_bl.txt

    table_name = configData.get_table_name()

    print("Start\n")

    if MyHdfsFile.isfile(a_client, file_name):
        if not configData.get_has_partition():
            sql = "LOAD DATA INPATH '{}' INTO TABLE {}".format(file_name, table_name)  # 'test.t1_trxrecprd_v2_zc'
            # '\' OVERWRITE INTO TABLE test.t1_trxrecprd_v2_bl2'
        else:
            sql = "LOAD DATA INPATH '{}' INTO TABLE {} PARTITION ( p_date='{}' )".format(file_name, table_name, p_date_str)  # 'test.t1_trxrecprd_v2_zc'
        print("OK" + "  " + sql+"\n")
        cur.execute(sql)  # , async=True)

    cur.close()
    conn.close()


def run_remove_files(configData: ConfigData):
    f_date_str = configData.get_f_date()  # StrTool.get_the_date_str(the_date, delta_day)  # "20181101"
    data_path = os.path.join(configData.get_data_path(), f_date_str)   # allinpay_data_bl
    utf8_path = os.path.join(configData.get_utf8_path(), f_date_str)   # allinpay_utf8_bl
    hdfs_path = str(pathlib.PurePosixPath(configData.get_hdfs_path()).joinpath(f_date_str))    # hdfs_dir_bl

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

    del_table = configData.get_table_name()   # "hive_table" + str(configData.the_id) # "rds_posflow.loginfo_rsp_bl"
    print(configData.cdh_ip()+del_table+f_date_str+configData.get_file_name(f_date_str)+configData.hive_ip())
    if not configData.get_has_partition():
        del_file = configData.get_file_name(f_date_str).replace('.', '*.')  # "file_ext" + str(configData.the_id)
        MyHdfsFile.delete_hive_ssh(configData.cdh_ip(), table=del_table, p_name=del_file, username=configData.cdh_user(), password=configData.cdh_pass())

    else:
        conn = connect(host=configData.hive_ip(), port=configData.hive_port(), auth_mechanism=configData.hive_auth(), user=configData.hive_user())
        cur = conn.cursor()

        # "ALTER TABLE rds_posflow.t1_trxrecprd_v2_tmp DROP IF EXISTS PARTITION(p_date='2019-02-08') "
        sql = "ALTER TABLE {} DROP IF EXISTS PARTITION( p_date='{}' )".format(del_table, p_date_str)
        print(sql)
        cur.execute(sql)

        cur.close()
        conn.close()


def run_unzip_file(configData: ConfigData):
    f_date_str = configData.get_f_date()  # "20181101"

    zip_path = os.path.join(configData.get_zip_path(), f_date_str)
    # root_path = configData.get_data("allinpay_data_zc")
    data_path = os.path.join(configData.get_data_path(), f_date_str)   # allinpay_data_zc

    #    ifile = '/home/testFolder/logflow/bl_shouyinbao/20181101/9999100000/t1_trxrecord_20181101_V2.csv'
    #    ofile = '/home/testFolder/logflow/bl_shouyinbao/UTF8/20181101/9999100000/t1_trxrecord_20181101_V2.csv'

    print("Start\n")

    # os.path.join(root_path, the_date) # real SYB folder don't have date folder

    f_name = configData.get_zip_name(f_date_str)  # 3= the_date+".zip" # 5 = the_date+"_agt.zip"
    a_file = os.path.join(zip_path, f_name)
    p_name = configData.get_file_name(f_date_str)  # p_date+"*"
    if MyLocalFile.check_file(a_file):
        MyLocalFile.unzip_the_file(a_file, data_path, p_name=p_name)


if __name__ == "__main__":
    m_is_test = False

    # client = Client(the_conf.hdfs_ip())  # "http://10.2.201.197:50070"

    if m_is_test:
        m_project_id = 3
        start_date_str = "20180901"
        m_days = 9

        m_project_id = StrTool.get_param_int(1, 3)
        start_date_str = StrTool.get_the_date_str(StrTool.get_param_str(2, ""))
        m_days = StrTool.get_param_int(3, 1)
    else:
        m_project_id = StrTool.get_param_int(1, 3)
        start_date_str = StrTool.get_the_date_str(StrTool.get_param_str(2, ""))
        m_days = StrTool.get_param_int(3, 1)

    start_date = StrTool.get_the_date(start_date_str)
    the_conf = ConfigData(m_project_id, StrTool.get_the_date_str_by_date(start_date, 0, 10), p_is_test=m_is_test)

    for i in range(0, m_days):
        delta = m_days - i - 1  # 不多加1天，20190108处理的是20190108文件夹
        # delta = days - i - 1 + 1  # 多加1天，是因为20190108处理的是20190107文件夹

        # 收银宝文件没有多 delta 1天
        # 1、20190110 191    2019-1-10    2018-7-4
        # 2、1 20180703 191  2018-7-3     2017-12-25 （2019-1-24 晚上）

        # 保理流水
        # 2、之前是到 20180702， 是先191天，之后手工多补了一天 20180702
        # 3、main3.py 7 20180702 70， 处理 20180702-20180423 （2019-1-25中午）
        # 4、main3.py 8 20180702 70， 处理 20180702-20180423 （2019-1-25中午）
        # 修改路径 remote_path_ftp_7="/ftpdata/thblposloan/posflow2/"
        # 5、main3.py 7 20180420 201      （2019-1-25中午）
        # 6、main3.py 8 20180420 201      （2019-1-25中午）

        # 保理风险
        # 3、2019-1-23 晚上已经补到 20180423
        # 修改路径 remote_path_ftp_9="/ftpdata/thblposloan/sftp/data/thblposloan/traderisk2/",
        # 7、main3.py 9 20180420 202      （2019-1-25中午）

        # Branch_APMS_2nd_20160701.txt  有多 delta 1天
        # 共执行了922天：因为缺了 20171010 的文件； 2016-10-19 有多的两个补数文件
        # 1、20190110 191    2019-1-9    2018-7-3
        # 3、20180703 265    2018-7-2    2017-10-11
        # 2、20171010 466    2017-10-9   2016-7-1

        # 其他文件没有多 delta 1天
        # 1、20190110 191    2019-1-10    2018-7-4 但实际看见的文件是到 2018-7-3 日
        # 再给保理7、8号两个流水增加1天文件2018-7-2，运行   7 20180703 1
        # 上面都是 205个文件
        # 再风险 4、6、9，  运行 4 20180702 190
        # 再流水 3、5、7，8  运行 3 20180702 190

        date2 = start_date - datetime.timedelta(days=delta)
        day_str2 = date2.strftime("%Y%m%d")

        the_conf.find_row(date2.strftime("%Y-%m-%d"))
        run_remove_files(the_conf)

        run_remove_hive(the_conf)

        run_sftp_file(the_conf)

        g_zip_path = the_conf.get_zip_path()
        if len(g_zip_path) > 0:
            run_unzip_file(the_conf)

        run_conv_file_local_to_hdfs(the_conf)
        run_hive(the_conf)

        run_remove_files(the_conf)

    print("ok")

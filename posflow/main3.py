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


def run_sftp_file(conf: ConfigData, the_date: str):
    the_date = StrTool.get_the_date_str(the_date)

    # allinpay_ftp_folder_bl_1 or allinpay_ftp_folder_bl_2
    f_dir = conf.get_remote_path_ftp(the_date)
    # allinpay_data_bl
    t_dir = os.path.join(conf.get_local_path_ftp(), the_date)
    # "file_ext" + str(conf.the_id)
    file_name = conf.get_ftp_name(the_date)

    # allinpay_ftp_ip_bl allinpay_ftp_port_bl allinpay_ftp_user_bl allinpay_ftp_pass_bl
    a = sftp_tool.Sftp_Tool(h=conf.get_ftp_ip(), p=int(conf.get_ftp_port()),
                            u=conf.get_ftp_user(), s=conf.get_ftp_pass(),
                            r=f_dir, d=t_dir)
    a.openSFTP()
    a.download_files(from_dir=f_dir, to_dir=t_dir, p_name=file_name)


def run_conv_file_local_to_hdfs(conf: ConfigData, the_date: str):
    """

    :param conf:
    :param the_date:
    :return:
    """
    the_date = StrTool.get_the_date_str(the_date)
    a_client = MyClient(conf.hdfs_ip())  # "http://10.2.201.197:50070"
    # allinpay_data_bl
    data_path = os.path.join(conf.get_data_path(), the_date)
    # allinpay_utf8_bl
    dest_dir1 = os.path.join(conf.get_utf8_path(), the_date)
    # hdfs_dir_bl
    dest_dir2 = str(pathlib.PurePosixPath(conf.get_hdfs_path()).joinpath(the_date))
    # file_ext7 = conf.get_data("file_ext7")  # _loginfo_rsp_bl_new.csv   # 20181101_loginfo_rsp_bl_new.csv
    # file_ext8 = conf.get_data("file_ext8")  # _rsp_agt_bl_new.del       # 20181101_rsp_agt_bl_new.del
    # file_ext9 = conf.get_data("file_ext9")  # _rxinfo_rsp_bl.txt        # 20181101_rxinfo_rsp_bl.txt

    # f_list = [file_ext7, file_ext8, file_ext9]

    print("Start\n")

    # "file_ext" + str(conf.the_id)
    file_name = conf.get_file_name(the_date).lower()
    files = MyLocalFile.get_child_file(data_path)
    for aFile in files:
        short_name = os.path.basename(aFile).lower()
        if short_name == file_name:
            to_file1 = str(pathlib.PurePath(dest_dir1).joinpath(pathlib.PurePath(aFile).name))
            to_file2 = str(pathlib.PurePosixPath(dest_dir2).joinpath(pathlib.PurePath(aFile).name))
            f_add_date = conf.get_hive_add_date(the_date)
            f_need_head = conf.get_hive_head()
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


def run_hive(conf: ConfigData, the_date: str):
    a_client = Client(conf.hdfs_ip())  # "http://10.2.201.197:50070"
    conn = connect(host=conf.hive_ip(), port=conf.hive_port(), auth_mechanism=conf.hive_auth(), user=conf.hive_user())
    cur = conn.cursor()

    the_date = StrTool.get_the_date_str(the_date)  # "20181101"
    # hdfs_dir_bl
    root_path = str(pathlib.PurePosixPath(conf.get_hdfs_path()).joinpath(the_date))  # "/data/posflow/allinpay_utf8_zc/20181101/"
    # file_ext7 = conf.get_data("file_ext7")  # _loginfo_rsp_bl_new.csv   # 20181101_loginfo_rsp_bl_new.csv
    # file_ext8 = conf.get_data("file_ext8")  # _rsp_agt_bl_new.del       # 20181101_rsp_agt_bl_new.del
    # file_ext9 = conf.get_data("file_ext9")  # _rxinfo_rsp_bl.txt        # 20181101_rxinfo_rsp_bl.txt

    print("Start\n")

    # file7 = str(pathlib.PurePosixPath(root_path).joinpath(the_date + file_ext7))
    # t_list = ["hive_table7", "hive_table8", "hive_table9"]

    # "file_ext" + str(conf.the_id)
    file_name = str(pathlib.PurePosixPath(root_path).joinpath(conf.get_file_name(the_date)))
    # "hive_table" + str(conf.the_id)
    table_name = conf.get_table_name()

    if MyHdfsFile.isfile(a_client, file_name):
            sql = 'LOAD DATA INPATH \'' + file_name + '\' INTO TABLE ' + table_name  # 'test.t1_trxrecprd_v2_zc'
            # '\' OVERWRITE INTO TABLE test.t1_trxrecprd_v2_bl2'
            print("OK" + "  " + sql+"\n")
            cur.execute(sql)  # , async=True)

    cur.close()
    conn.close()


def run_remove_files(conf: ConfigData, the_date: str, delta_day=0):
    f_date_str = StrTool.get_the_date_str(the_date, delta_day)   # "20181101"
    data_path = os.path.join(conf.get_data_path(), f_date_str)   # allinpay_data_bl
    utf8_path = os.path.join(conf.get_utf8_path(), f_date_str)   # allinpay_utf8_bl
    hdfs_path = str(pathlib.PurePosixPath(conf.get_hdfs_path()).joinpath(f_date_str))    # hdfs_dir_bl

    a_client = MyClient(conf.hdfs_ip())  # "http://10.2.201.197:50070"

    shutil.rmtree(data_path, ignore_errors=True)
    shutil.rmtree(utf8_path, ignore_errors=True)
    a_client.delete(hdfs_path, recursive=True)


def run_remove_hive(conf: ConfigData, the_date: str, delta_day=0):
    f_date_str = StrTool.get_the_date_str(the_date, delta_day)  # "20181101"
    # del_table7 = conf.get_data("hive_table7") # "rds_posflow.loginfo_rsp_bl"
    # del_file7 = the_date + conf.get_data("file_ext7").replace('.', '*.')

    del_table = conf.get_table_name()   # "hive_table" + str(conf.the_id)
    del_file = conf.get_file_name(f_date_str).replace('.', '*.')  # "file_ext" + str(conf.the_id)

    MyHdfsFile.delete_hive_ssh(conf.get_data("cdh_ip"), table=del_table, p_name=del_file, username=conf.get_data("cdh_user"), password=conf.get_data("cdh_pass"))


def run_unzip_file(conf: ConfigData, p_date: str):
    p_date = StrTool.get_the_date_str(p_date)
    if (type(p_date) is str) and len(p_date) == 8:
        m_month = p_date[0:6]
        m_day = p_date[6:8]
    else:
        return

    p_zip_path = os.path.join(conf.get_zip_path(), p_date)
    # root_path = conf.get_data("allinpay_data_zc")
    data_path = os.path.join(conf.get_data_path(), p_date)   # allinpay_data_zc

    #    ifile = '/home/testFolder/logflow/bl_shouyinbao/20181101/9999100000/t1_trxrecord_20181101_V2.csv'
    #    ofile = '/home/testFolder/logflow/bl_shouyinbao/UTF8/20181101/9999100000/t1_trxrecord_20181101_V2.csv'

    print("Start\n")

    # os.path.join(root_path, the_date) # real SYB folder don't have date folder

    p_f_name = conf.get_zip_name(p_date)  # 3= the_date+".zip" # 5 = the_date+"_agt.zip"
    p_a_file = os.path.join(p_zip_path, p_f_name)
    p_p_name = conf.get_file_name(p_date)  # p_date+"*"
    if MyLocalFile.check_file(p_a_file):
        MyLocalFile.unzip_the_file(p_a_file, data_path, p_name=p_p_name)


if __name__ == "__main__":
    the_conf = ConfigData(p_is_test=False)

    # client = Client(the_conf.hdfs_ip())  # "http://10.2.201.197:50070"

    if the_conf.is_test():
        day_str = the_conf.test_date()
        days = 9
    else:
        the_conf.m_project_id = StrTool.get_param_int(1, 10)
        day_str = StrTool.get_param_str(2, "")
        days = StrTool.get_param_int(3, 1)

    f_delta = the_conf.get_data("file_date_delta"+str(the_conf.m_project_id), "0")

    day_str = StrTool.get_the_date_str(day_str, - int(f_delta))

    date1 = StrTool.get_the_date(day_str)
    for i in range(0, days):
        delta = days - i - 1  # 不多加1天，20190108处理的是20190108文件夹
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


        date2 = date1 - datetime.timedelta(days=delta)
        day_str2 = date2.strftime("%Y%m%d")
        run_remove_files(the_conf, day_str2, 0)
        run_remove_hive(the_conf, day_str2, 0)

        run_sftp_file(the_conf, day_str2)

        g_zip_path = the_conf.get_zip_path()
        if len(g_zip_path) > 0:
            run_unzip_file(the_conf, p_date=day_str2)

        run_conv_file_local_to_hdfs(the_conf, the_date=day_str2)
        run_hive(the_conf, the_date=day_str2)

        run_remove_files(the_conf, day_str2, 0)

    print("ok")

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
from wj_tools.file_check import myLocalFile
from wj_tools.file_check import myHdfsFile
from wj_tools.hdfsclient import MyClient  # hdfs
# hive
# data path config file
from conf import ConfigData as cF
from wj_tools import datestr


def run_unzip_file(the_date, folder_type=2, is_baoli=True):
    the_date = datestr.getTheDateStr(the_date)
    if (type(the_date) is str) and len(the_date) == 8:
        m_month = the_date[0:6]
        m_day = the_date[6:8]
    else:
        return

    root_path = cF.root_path(is_baoli)
    destdir = cF.unzip_dir(is_baoli)
    # root_path = "/media/bd/data/toshiba/data/SYB/"
    # destdir = "/home/bd/桌面/201811_flow/bl_shouyinbao/UNZIP"

    #    ifile = '/home/testFolder/logflow/bl_shouyinbao/20181101/9999100000/t1_trxrecord_20181101_V2.csv'
    #    ofile = '/home/testFolder/logflow/bl_shouyinbao/UTF8/20181101/9999100000/t1_trxrecord_20181101_V2.csv'
    filepre = cF.file_pre1()  # "t1_trxrecord_"
    fileext = cF.file_ext1()  # "_v2.zip"

    print("Start\n")

    # os.path.join(root_path, the_date) # real SYB folder don't have date folder
    branchs = myLocalFile.getchild(root_path)
    for aBranch in branchs:
        if myLocalFile.checkbranch(aBranch):
            months = myLocalFile.getchild(aBranch)
            for aMonth in months:
                the_month = myLocalFile.checkmonth(aMonth)
                if the_month > 0 and "{:0>6d}".format(the_month) == m_month:
                    days = myLocalFile.getchild(aMonth)
                    for aDay in days:
                        the_day = myLocalFile.checkday(aDay)
                        if the_day > 0 and "{:0>2d}".format(the_day) == m_day:
                            files = myLocalFile.getchild(aDay)
                            for aFile in files:
                                if myLocalFile.checkfile(aFile, start=filepre, ext=fileext):
                                    shortname = os.path.basename(aBranch)
                                    if folder_type == 1:
                                        newpath = os.path.join(destdir, shortname, m_month, m_day)
                                        # "{:0>6d}".format(month)  "{:0>2d}".format(day)
                                    else:
                                        newpath = os.path.join(destdir, m_month + m_day, shortname)
                                        # "{:0>6d}{:0>2d}".format(month, day)
                                    myLocalFile.unzipTheFile(aFile, newpath)


def conv_file_local(from_file, to_file, need_first_line=False):
    if not os.path.isfile(from_file):
        return
    newpath = os.path.dirname(to_file)
    pathlib.Path(newpath).mkdir(parents=True, exist_ok=True)

    f1 = open(from_file, 'r', encoding="gb18030")
    f2 = open(to_file, 'w', encoding="utf-8")
    data = '  '
    idn = 0
    while len(data) > 0:
        data = f1.readline()
        #  print(data + "\n\n")
        #  print(idn)
        #        code = chardet.detect(dd)['encoding']
        #        print(code)
        #        middle = dd.decode("gb18030")  # gb2312
        #        ss = middle.encode("utf-8")
        #        ss = dd.encode("utf-8")
        #        print(dd)
        #        if idn > 0 or platform.platform().lower().startswith("linux"):
        #            f2.write(data)
        if idn > 0 or need_first_line:
            f2.write(data)
        # print("\nWrite")
        idn = idn + 1
    print("Write " + to_file + "\n")
    f1.close()
    f2.close()
    print("ok")


def conv_file_hdfs(from_file, to_file, client):
    if not os.path.isfile(from_file):
        return
    # newpath = os.path.dirname(to_file)
    # pathlib.Path(newpath).mkdir(parents=True, exist_ok=True)
    myHdfsFile.safeMakedir(client, to_file)

    f1 = open(from_file, 'r', encoding="gb18030")
    #    f2 = open(to_file, 'w', encoding="utf-8")
    data = '  '
    idn = 0
    while len(data) > 0:
        data = f1.readline()
        print(data + "\n\n")
        print(idn)
        if idn > 0:
            # client.write(hdfs_path, data, overwrite=True, append=False) # first line
            # client.write(hdfs_path, data, overwrite=False, append=True) # rest line
            client.write(to_file, data.encode('utf-8'), overwrite=(idn == 1), append=(not (idn == 1)))
            print("\nWrite")
        idn = idn + 1
    client.set_permission(to_file, 777)
    f1.close()
    print("ok")


def run_conv_file_local(is_baoli=True):
    the_date = cF.test_date()  # "20181101"
    root_path = cF.unzip_dir(is_baoli)
    dest_dir = cF.decode_dir(is_baoli)

    # root_path = "/home/bd/桌面/201811_flow/bl_shouyinbao/UNZIP/"
    # dest_dir = "/home/bd/桌面/201811_flow/bl_shouyinbao/UTF8/"
    # root_path = 'D:/DATA/UNZIP/'
    # dest_dir = 'D:/DATA/UTF8/'
    # root_path = "/home/testFolder/logflow/bl_shouyinbao/"
    # dest_dir = "/home/testFolder/logflow/bl_shouyinbao/UTF8/"

    #    ifile = '/home/testFolder/logflow/bl_shouyinbao/20181101/9999100000/t1_trxrecord_20181101_V2.csv'
    #    ofile = '/home/testFolder/logflow/bl_shouyinbao/UTF8/20181101/9999100000/t1_trxrecord_20181101_V2.csv'
    file_pre = cF.file_pre1()  # "t1_trxrecord_"
    file_ext = cF.file_ext2()  # "_V2.csv"

    print("Start\n")

    branchs = myLocalFile.getchild(os.path.join(root_path, the_date))
    for aBranch in branchs:
        if myLocalFile.checkbranch(aBranch):
            files = myLocalFile.getchild(aBranch)
            for aFile in files:
                if myLocalFile.checkfile(aFile, file_pre + the_date, file_ext):
                    # '/home/testFolder/logflow/bl_shouyinbao/20181101/9999100000/t1_trxrecord_20181101_V2.csv'
                    # '/home/testFolder/logflow/bl_shouyinbao/UTF8/20181101/9999100000/t1_trxrecord_20181101_V2.csv'
                    conv_file_local(aFile, os.path.join(dest_dir, the_date, os.path.basename(aBranch), file_pre + the_date + file_ext), True)


def run_conv_file_hdfs(is_baoli=True):
    the_date = cF.test_date()  # "20181101"
    client = Client(cF.hdfs_ip())  # "http://10.2.201.197:50070"
    root_path = cF.unzip_dir(is_baoli)     # 'D:/DATA/UNZIP/'
    dest_dir = cF.hdfs_dir(is_baoli)

    file_pre = cF.file_pre1()  # "t1_trxrecord_"
    file_ext = cF.file_ext2()  # "_V2.csv"

    #    client.upload('/shouyinbao/', "/home/testFolder/logflow/bl_shouyinbao/UTF8/20181101/9999100000/t1_trxrecord_20181101_V2.csv", cleanup=True)
    #   dat = client.list('/shouyinbao/', status=False)
    #   print(dat)

    # root_path = "/home/bd/桌面/201811_flow/zc_shouyinbao/UNZIP/"
    # dest_dir = "/shouyinbao/zc_shouyinbao/UTF8/"
    # root_path = "/home/testFolder/logflow/bl_shouyinbao/UNZIP/"
    # dest_dir = "/home/testFolder/logflow/bl_shouyinbao/UTF8/"

    #    ifile = '/home/testFolder/logflow/bl_shouyinbao/20181101/9999100000/t1_trxrecord_20181101_V2.csv'
    #    ofile = '/home/testFolder/logflow/bl_shouyinbao/UTF8/20181101/9999100000/t1_trxrecord_20181101_V2.csv'

    print("Start\n")

    branchs = myLocalFile.getchild(os.path.join(root_path, the_date))
    for aBranch in branchs:
        if myLocalFile.checkbranch(aBranch):
            files = myLocalFile.getchild(aBranch)
            for aFile in files:
                if myLocalFile.checkfile(aFile, file_pre + the_date, file_ext):
                    # '/home/testFolder/logflow/bl_shouyinbao/20181101/9999100000/t1_trxrecord_20181101_V2.csv'
                    # '/home/testFolder/logflow/bl_shouyinbao/UTF8/20181101/9999100000/t1_trxrecord_20181101_V2.csv'
                    conv_file_hdfs(aFile,
                                   os.path.join(dest_dir, the_date, os.path.basename(aBranch), file_pre + the_date + file_ext),
                                   client)


def run_conv_file_local_to_hdfs(the_date, is_baoli=True):
    the_date = datestr.getTheDateStr(the_date)  # "20181101"
    client = MyClient(cF.hdfs_ip())  # "http://10.2.201.197:50070"
    root_path = cF.unzip_dir(is_baoli)
    dest_dir1 = cF.decode_dir(is_baoli)
    dest_dir2 = cF.hdfs_dir(is_baoli)
    file_pre = cF.file_pre1()  # "t1_trxrecord_"
    file_ext = cF.file_ext2()  # "_V2.csv"

    #    client.upload('/shouyinbao/', "/home/testFolder/logflow/bl_shouyinbao/UTF8/20181101/9999100000/t1_trxrecord_20181101_V2.csv", cleanup=True)
    #   dat = client.list('/shouyinbao/', status=False)
    #   print(dat)

    # root_path = "/home/bd/桌面/201811_flow/zc_shouyinbao/UNZIP/"
    # dest_dir1 = "/home/bd/桌面/201811_flow/zc_shouyinbao/UTF8/"
    # dest_dir2 = "/shouyinbao/zc_shouyinbao/UTF8/"

    # root_path = "/home/testFolder/logflow/bl_shouyinbao/UNZIP/"
    # destdir = "/home/testFolder/logflow/bl_shouyinbao/UTF8/"

    #    ifile = '/home/testFolder/logflow/bl_shouyinbao/20181101/9999100000/t1_trxrecord_20181101_V2.csv'
    #    ofile = '/home/testFolder/logflow/bl_shouyinbao/UTF8/20181101/9999100000/t1_trxrecord_20181101_V2.csv'

    print("Start\n")

    branchs = myLocalFile.getchild(os.path.join(root_path, the_date))
    for aBranch in branchs:
        if myLocalFile.checkbranch(aBranch):
            files = myLocalFile.getchild(aBranch)
            for aFile in files:
                if myLocalFile.checkfile(aFile, file_pre + the_date, file_ext):
                    # '/home/testFolder/logflow/bl_shouyinbao/20181101/9999100000/t1_trxrecord_20181101_V2.csv'
                    # '/home/testFolder/logflow/bl_shouyinbao/UTF8/20181101/9999100000/t1_trxrecord_20181101_V2.csv'
                    to_file1 = os.path.join(dest_dir1, the_date, os.path.basename(aBranch), file_pre + the_date + file_ext)
                    to_file2 = os.path.join(dest_dir2, the_date, os.path.basename(aBranch), file_pre + the_date + file_ext)
                    conv_file_local(aFile, to_file1, need_first_line=False)
                    myHdfsFile.safeMakedir(client, to_file2)
                    # client.newupload(to_file2, to_file1, encoding='utf-8')
                    thefile = client.status(to_file2, strict=False)
                    if thefile is None:
                        client.upload(to_file2, to_file1)
                        client.set_permission(to_file2, 777)
                    # client.set_owner(thePath,owner='hdfs',group='supergroup')
                    elif thefile['type'].lower() == 'file':  # 'directory'
                        client.set_permission(to_file2, 777)


def run_hdfs_test():
    # the_date = cF.test_date()  # "20181101"
    client = Client(cF.hdfs_ip())  # "http://10.2.201.197:50070"
    # root_path = cF.unzip_dir(is_baoli)     # 'D:/DATA/UNZIP/'
    # dest_dir = cF.hdfs_dir(is_baoli)

    # file_pre = cF.file_pre1()  # "t1_trxrecord_"
    # file_ext = cF.file_ext2()  # "_V2.csv"

    #    client.upload('/shouyinbao/', "/home/testFolder/logflow/bl_shouyinbao/UTF8/20181101/9999100000/t1_trxrecord_20181101_V2.csv", cleanup=True)
    dat = client.list('/', status=False)
    print(dat)


def run_hive_test():
    host = cF.hive_ip()    # '10.2.201.197'
    port = cF.hive_port()  # 10000
    user = cF.hive_user()  # "hdfs"
    auth = cF.hive_auth()  # 'PLAIN'
    test = cF.hive_test()  # "select * from test.test1"

    conn = connect(host=host, port=port, auth_mechanism=auth, user=user, password='Redhat@20161')
    cur = conn.cursor()

    cur.execute(test)
    data = as_pandas(cur)
    print(data)

    cur.close()
    conn.close()


def run_remove_files(the_date, delta_day=0):
    sdate = datestr.getTheDateStr(the_date, delta_day)  # "20181101"
    zip_path = os.path.join(cF.unzip_dir(), sdate)
    utf8_path = os.path.join(cF.decode_dir(), sdate)
    hdfs_path = os.path.join(cF.hdfs_dir(), sdate)
    shutil.rmtree(zip_path, ignore_errors=True)
    shutil.rmtree(utf8_path, ignore_errors=True)
    client = MyClient(cF.hdfs_ip())  # "http://10.2.201.197:50070"
    client.delete(hdfs_path, recursive=True)


def run_hive(the_date, is_baoli=True):
    client = Client(cF.hdfs_ip())  # "http://10.2.201.197:50070"
    conn = connect(host=cF.hive_ip(), port=cF.hive_port(), auth_mechanism='PLAIN', user=cF.hive_user())
    cur = conn.cursor()

    the_date = datestr.getTheDateStr(the_date)  # "20181101"
    root_path = cF.hdfs_dir(is_baoli)  # "/shouyinbao/bl_shouyinbao/UTF8/"
    file_pre = cF.file_pre1()  # "t1_trxrecord_"
    file_ext = cF.file_ext1()  # "_V2.csv"

    print("Start\n")

    idn = 0
    branchs = myHdfsFile.getchild(client, root_path + the_date)
    for aBranch in branchs:
        if myHdfsFile.checkbranch(client, aBranch):
            files = myHdfsFile.getchild(client, aBranch)
            for aFile in files:
                if myHdfsFile.checkfile(client, aFile, file_pre + the_date, file_ext):
                    # '/shouyinbao/bl_shouyinbao/UTF8/20181101/9999997900/t1_trxrecord_20181101_V2.csv'
                    sql = 'LOAD DATA INPATH \'' +\
                          os.path.join(root_path, the_date, os.path.basename(aBranch), file_pre + the_date + file_ext)
                    sql += '\' INTO TABLE ' + cF.hive_table(is_baoli)  # 'test.t1_trxrecprd_v2_zc'
                    # '\' OVERWRITE INTO TABLE test.t1_trxrecprd_v2_bl2'
                    idn += 1
                    print(str(idn) + "  " + sql+"\n")
                    cur.execute(sql)  # , async=True)

    cur.close()
    conn.close()

# SELECT count(*) from t1_trxrecprd_v2_bl; 	5199590 # bad coding ,deleted
# SELECT count(*) from t1_trxrecprd_v2_bl2; 		99793  # one day
# select count(*) from test.t1_trxrecprd_V2_bl2; 5199590


if __name__ == "__main__":

    #    return_code = subprocess.call("./ftpcmd.sh", shell=True)
    #    print(return_code)

    if cF.is_test():
        the_date1 = cF.test_date()
    else:
        the_date1 = ''

    run_hdfs_test()
    run_hive_test()

    run_remove_files(the_date1, 0)
    run_unzip_file(the_date=the_date1)  # "20181101"
    #    runConvFileLocal() # not use
    #    runConvFileHDFS()  # not use
    #    runHiveTest()
    run_conv_file_local_to_hdfs(the_date=the_date1, is_baoli=False)
    run_hive(the_date=the_date1, is_baoli=False)
    for i in range(-45, -15):
        run_remove_files(the_date1, i)
    print("ok")

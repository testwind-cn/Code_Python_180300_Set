#!coding:utf-8
import sys
import os
import shutil
import pathlib
import zipfile
import subprocess
import platform
from wj_tools import sftp_tool
# hdfs
from hdfs.client import Client
# hdfs
# hive
from impala.dbapi import connect
from impala.util import as_pandas
import pandas
from wj_tools.file_check import myLocalFile
from wj_tools.file_check import myHdfsFile
from wj_tools.hdfsclient import MyClient # hdfs
# hive
# data path config file
from conf import conf_data as cf
from wj_tools import datestr


def runSFTP_File(thedate):
    thedate = datestr.getTheDateStr(thedate)
    if (type(thedate) is str) and len(thedate) == 8:
        m_Month = thedate[0:6]
        m_Day = thedate[6:8]
    else:
        return

    a = sftp_tool.Sftp_Tool(h="172.31.130.14", p=22, u="root", s="Redhat@2016", r="/ftpdata/thblposloan/posflow",d="/ftpdata/thblposloan/posflow")
    a.openSFTP()
    a.download_files(from_dir=os.path.join("/ftpdata/thblposloan/posflow", "20181101"),
                     to_dir="")
    b.downloadFilesByDay(to_dir="", day_str="20181101", from_dir=os.path.join("/ftpdata/thblposloan/posflow", "20181101"))


def convFileLocal(fromF, toFile, needFirstLine= False):
    if not os.path.isfile(fromF):
        return
    newpath = os.path.dirname(toFile)
    pathlib.Path(newpath).mkdir(parents=True, exist_ok=True)

    f1 = open(fromF, 'r', encoding="gb18030")
    f2 = open(toFile, 'w', encoding="utf-8")
    data = '  '
    i = 0
    while len(data) > 0:
        data = f1.readline()
        #  print(data + "\n\n")
        #  print(i)
        #        code = chardet.detect(dd)['encoding']
        #        print(code)
        #        middle = dd.decode("gb18030")  # gb2312
        #        ss = middle.encode("utf-8")
        #        ss = dd.encode("utf-8")
        #        print(dd)
        #        if i > 0 or platform.platform().lower().startswith("linux"):
        #            f2.write(data)
        if i > 0 or needFirstLine:
            f2.write(data)
        # print("\nWrite")
        i = i + 1
    print("Write " + toFile + "\n")
    f1.close()
    f2.close()
    print("ok")


def convFileHDFS(fromF, toFile, client):
    if not os.path.isfile(fromF):
        return
    #newpath = os.path.dirname(toFile)
    #pathlib.Path(newpath).mkdir(parents=True, exist_ok=True)
    myHdfsFile.safeMakedir(client, toFile)

    f1 = open(fromF, 'r', encoding="gb18030")
    #    f2 = open(toFile, 'w', encoding="utf-8")
    data = '  '
    i = 0
    while len(data) > 0:
        data = f1.readline()
        print(data + "\n\n")
        print(i)
        if i > 0:
            # client.write(hdfs_path, data, overwrite=True, append=False) # first line
            # client.write(hdfs_path, data, overwrite=False, append=True) # rest line
            client.write(toFile, data.encode('utf-8'), overwrite=(i == 1), append=(not (i == 1)))
            print("\nWrite")
        i = i + 1
    client.set_permission(toFile,777)
    f1.close()
    print("ok")


def runConvFileLocal(isBaoli=True):
    thedate = cf.testdate() #"20181101"
    root_path = cf.unzip_dir(isBaoli)
    destdir = cf.decode_dir(isBaoli)

    # root_path = "/home/bd/桌面/201811_flow/bl_shouyinbao/UNZIP/"
    # destdir = "/home/bd/桌面/201811_flow/bl_shouyinbao/UTF8/"
    # root_path = 'D:/DATA/UNZIP/'
    # destdir = 'D:/DATA/UTF8/'
    # root_path = "/home/testFolder/logflow/bl_shouyinbao/"
    # destdir = "/home/testFolder/logflow/bl_shouyinbao/UTF8/"

    #    ifile = '/home/testFolder/logflow/bl_shouyinbao/20181101/9999100000/t1_trxrecord_20181101_V2.csv'
    #    ofile = '/home/testFolder/logflow/bl_shouyinbao/UTF8/20181101/9999100000/t1_trxrecord_20181101_V2.csv'
    filepre = cf.filepre1()  #"t1_trxrecord_"
    fileext = cf.fileext2()  #"_V2.csv"

    print("Start\n")

    branchs = myLocalFile.getchild(os.path.join(root_path,thedate))
    for aBranch in branchs:
        if myLocalFile.checkbranch(aBranch):
            files = myLocalFile.getchild(aBranch)
            for aFile in files:
                if myLocalFile.checkfile(aFile, filepre + thedate, fileext):
                    # '/home/testFolder/logflow/bl_shouyinbao/20181101/9999100000/t1_trxrecord_20181101_V2.csv'
                    # '/home/testFolder/logflow/bl_shouyinbao/UTF8/20181101/9999100000/t1_trxrecord_20181101_V2.csv'
                    convFileLocal(aFile, os.path.join(destdir , thedate , os.path.basename(aBranch) , filepre + thedate + fileext),True)


def runConvFileHDFS(isBaoli=True):
    thedate = cf.testdate() #"20181101"
    client = Client(cf.hdfs_IP() ) # "http://10.2.201.197:50070"
    root_path = cf.unzip_dir(isBaoli)     # 'D:/DATA/UNZIP/'
    destdir = cf.hdfs_dir(isBaoli)

    filepre = cf.filepre1()  # "t1_trxrecord_"
    fileext = cf.fileext2()  # "_V2.csv"

    #    client.upload('/shouyinbao/', "/home/testFolder/logflow/bl_shouyinbao/UTF8/20181101/9999100000/t1_trxrecord_20181101_V2.csv", cleanup=True)
    #   dat = client.list('/shouyinbao/', status=False)
    #   print(dat)

    # root_path = "/home/bd/桌面/201811_flow/zc_shouyinbao/UNZIP/"
    # destdir = "/shouyinbao/zc_shouyinbao/UTF8/"
    # root_path = "/home/testFolder/logflow/bl_shouyinbao/UNZIP/"
    # destdir = "/home/testFolder/logflow/bl_shouyinbao/UTF8/"

    #    ifile = '/home/testFolder/logflow/bl_shouyinbao/20181101/9999100000/t1_trxrecord_20181101_V2.csv'
    #    ofile = '/home/testFolder/logflow/bl_shouyinbao/UTF8/20181101/9999100000/t1_trxrecord_20181101_V2.csv'


    print("Start\n")

    branchs = myLocalFile.getchild(os.path.join(root_path ,thedate))
    for aBranch in branchs:
        if myLocalFile.checkbranch(aBranch):
            files = myLocalFile.getchild(aBranch)
            for aFile in files:
                if myLocalFile.checkfile(aFile, filepre + thedate, fileext):
                    # '/home/testFolder/logflow/bl_shouyinbao/20181101/9999100000/t1_trxrecord_20181101_V2.csv'
                    # '/home/testFolder/logflow/bl_shouyinbao/UTF8/20181101/9999100000/t1_trxrecord_20181101_V2.csv'
                    convFileHDFS(aFile, os.path.join(destdir , thedate ,
                                 os.path.basename(aBranch) , filepre + thedate + fileext ),
                                 client)


# 1?óúpython2ù×÷hdfsμ?API?éò?2é?′1ùí?:
# https://hdfscli.readthedocs.io/en/latest/api.html
# https://hdfscli.readthedocs.io/en/latest/api.html#module-hdfs.client
# client.makedirs(hdfs_path)
# client.delete(hdfs_path)
# client.download(hdfs_path, local_path, overwrite=False)
# client.write(hdfs_path, data, overwrite=False, append=True)
# client.write(hdfs_path, data, overwrite=True, append=False)
# client.rename(hdfs_src_path, hdfs_dst_path)
# client.list(hdfs_path, status=False)
# ?áè?hdfs???t?úèY,????DD′?è?êy×é·μ??
#     lines = []
#     with client.read(filename, encoding='utf-8', delimiter='\n') as reader:
#         for line in reader:
#             # pass
#             # print line.strip()
#             lines.append(line.strip())
#     return lines

def runConvFileLocalToHDFS(thedate, isBaoli=True):
    thedate = datestr.getTheDateStr(thedate)  # "20181101"
    client = MyClient(cf.hdfs_IP() ) # "http://10.2.201.197:50070"
    root_path = cf.unzip_dir(isBaoli)
    destdir1 = cf.decode_dir(isBaoli)
    destdir2 = cf.hdfs_dir(isBaoli)
    filepre = cf.filepre1() #"t1_trxrecord_"
    fileext = cf.fileext2() #"_V2.csv"

    #    client.upload('/shouyinbao/', "/home/testFolder/logflow/bl_shouyinbao/UTF8/20181101/9999100000/t1_trxrecord_20181101_V2.csv", cleanup=True)
    #   dat = client.list('/shouyinbao/', status=False)
    #   print(dat)

    # root_path = "/home/bd/桌面/201811_flow/zc_shouyinbao/UNZIP/"
    # destdir1 = "/home/bd/桌面/201811_flow/zc_shouyinbao/UTF8/"
    # destdir2 = "/shouyinbao/zc_shouyinbao/UTF8/"

    # root_path = "/home/testFolder/logflow/bl_shouyinbao/UNZIP/"
    # destdir = "/home/testFolder/logflow/bl_shouyinbao/UTF8/"

    #    ifile = '/home/testFolder/logflow/bl_shouyinbao/20181101/9999100000/t1_trxrecord_20181101_V2.csv'
    #    ofile = '/home/testFolder/logflow/bl_shouyinbao/UTF8/20181101/9999100000/t1_trxrecord_20181101_V2.csv'


    print("Start\n")

    branchs = myLocalFile.getchild(os.path.join(root_path, thedate))
    for aBranch in branchs:
        if myLocalFile.checkbranch(aBranch):
            files = myLocalFile.getchild(aBranch)
            for aFile in files:
                if myLocalFile.checkfile(aFile, filepre + thedate, fileext):
                    # '/home/testFolder/logflow/bl_shouyinbao/20181101/9999100000/t1_trxrecord_20181101_V2.csv'
                    # '/home/testFolder/logflow/bl_shouyinbao/UTF8/20181101/9999100000/t1_trxrecord_20181101_V2.csv'
                    toFile1 = os.path.join(destdir1, thedate, os.path.basename(aBranch), filepre + thedate + fileext)
                    toFile2 = os.path.join(destdir2, thedate, os.path.basename(aBranch), filepre + thedate + fileext)
                    convFileLocal(aFile, toFile1,needFirstLine= False)
                    myHdfsFile.safeMakedir(client, toFile2)
                    # client.newupload(toFile2, toFile1, encoding='utf-8')
                    thefile = client.status(toFile2, strict=False)
                    if thefile is None:
                        client.upload(toFile2, toFile1)
                        client.set_permission(toFile2, 777)
                    # client.set_owner(thePath,owner='hdfs',group='supergroup')
                    elif thefile['type'].lower() == 'file':  # 'directory'
                        client.set_permission(toFile2, 777)


def runHiveTest():
    host = cf.hive_IP()      #'10.2.201.197'
    port = cf.hive_port() # 10000
    user = cf.hive_user() # "hdfs"
    auth = cf.hive_auth() #'PLAIN'
    test = cf.hive_test() # "select * from test.test1"

    conn = connect(host=host, port=port, auth_mechanism=auth, user=user,password='Redhat@20161')
    cur = conn.cursor()

    cur.execute(test)
    data = as_pandas(cur)
    print(data)

    cur.close()
    conn.close()

def runRemoveFiles(thedate, deltaDay=0):
    sdate = datestr.getTheDateStr(thedate,deltaDay)  # "20181101"
    zip_path = os.path.join( cf.unzip_dir(), sdate )
    utf8_path = os.path.join( cf.decode_dir(), sdate )
    hdfs_path = os.path.join( cf.hdfs_dir(), sdate )
    shutil.rmtree(zip_path,ignore_errors=True)
    shutil.rmtree(utf8_path,ignore_errors=True)
    client = MyClient(cf.hdfs_IP())  # "http://10.2.201.197:50070"
    client.delete(hdfs_path, recursive=True)


def runHive(thedate, isBaoli=True):
    client = Client(cf.hdfs_IP() ) # "http://10.2.201.197:50070"
    conn = connect(host=cf.hive_IP(), port=cf.hive_port(), auth_mechanism='PLAIN', user=cf.hive_user())
    cur = conn.cursor()

    tthedate = datestr.getTheDateStr(thedate)  # "20181101"
    root_path = cf.hdfs_dir(isBaoli) #"/shouyinbao/bl_shouyinbao/UTF8/"
    filepre = cf.filepre1()  # "t1_trxrecord_"
    fileext = cf.fileext1()  # "_V2.csv"

    print("Start\n")

    i = 0
    branchs = myHdfsFile.getchild(client, root_path + thedate)
    for aBranch in branchs:
        if myHdfsFile.checkbranch(client, aBranch):
            files = myHdfsFile.getchild(client, aBranch)
            for aFile in files:
                if myHdfsFile.checkfile(client, aFile, filepre + thedate, fileext):
                    # '/shouyinbao/bl_shouyinbao/UTF8/20181101/9999997900/t1_trxrecord_20181101_V2.csv'
                    sql = 'LOAD DATA INPATH \'' +\
                          os.path.join(root_path,thedate,os.path.basename(aBranch), filepre + thedate + fileext)
                    sql += '\' INTO TABLE '+ cf.hive_table(isBaoli) # 'test.t1_trxrecprd_v2_zc'
                    # '\' OVERWRITE INTO TABLE test.t1_trxrecprd_v2_bl2'
                    i += 1
                    print(str(i)+ "  " + sql+"\n")
                    cur.execute(sql)  # , async=True)

    cur.close()
    conn.close()

# SELECT count(*) from t1_trxrecprd_v2_bl; 	5199590 # bad coding ,deleted
# SELECT count(*) from t1_trxrecprd_v2_bl2; 		99793  # one day
# select count(*) from test.t1_trxrecprd_V2_bl2; 5199590

if __name__ == "__main__":

#    return_code = subprocess.call("./ftpcmd.sh", shell=True)
#    print(return_code)

    if 1 == 1: #cf.isTest():
        thedate = cf.testdate()
    else:
        thedate = ''

    runRemoveFiles(thedate,0)
    runSFTP_File(thedate=thedate)  #"20181101"
    #    runConvFileLocal() # not use
    #    runConvFileHDFS()  # not use
    #    runHiveTest()
    runConvFileLocalToHDFS(thedate=thedate,isBaoli=False)
    runHive(thedate=thedate,isBaoli=False)
    for i in range(-45,-15):
        runRemoveFiles(thedate, i)
    print("ok")

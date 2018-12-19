#!coding:utf-8
import sys
import os, stat
import pathlib
import zipfile
import platform
from wj_tools.mylog import myLog

class myLocalFile:

    @staticmethod
    def safeMakedir(dirStr: str, mode: int = 0) -> object:
        """stat.S_IRWXO + stat.S_IRWXG + stat.S_IRWXU

        :param dirStr:
        :param mode:
        :return:
        """
        try:
            if not os.path.exists(dirStr):
                # os.makedirs(dirStr)
                pathlib.Path(dirStr).mkdir(parents=True, exist_ok=True)
                if mode > 0:
                    os.chmod(dirStr, mode)
        except Exception as e:
            myLog.Log("safeMakedir Error: " + str(e), False)
        return

    @staticmethod
    def isdirOrFile(path, type=3):  # 1 file , 2 dir, 3 any
        if (type & 1) > 0 and os.path.isfile(path):
            return True
        if (type & 2) > 0 and os.path.isdir(path):
            return True
        return False

    @staticmethod
    def isfile(path):
        return os.path.isfile(path)

    @staticmethod
    def isdir(path):
        return os.path.isdir(path)

    @staticmethod
    def checkname(name, start: str="", ext: str="", fstr: str= '', default: bool=True):
        """

        :param name:
        :param start:
        :param ext:
        :param fstr:
        :param default:
        :return:isIn, isDefault
        """
        if type(name) is not str or len(name) <= 0:
            return default, True

        if type(start) is not str:
            start = ""

        if type(ext) is not str:
            ext = ""

        if type(fstr) is not str:
            fstr = ""

        d1 = (len(start) <= 0)
        d2 = (len(ext) <= 0)
        d3 = (len(fstr) <= 0)

        if d1 and d2 and d3:
            return default, True

        v1 = len(start) > 0 and name.lower().startswith(start.lower())
        v2 = len(ext) > 0 and name.lower().endswith(ext.lower())
        v3 = len(fstr) > 0 and (name.lower().find(fstr.lower()) >= 0)

        if default:
            if (v1 or d1) and (v2 or d2) and (v3 or d3):
                return True, False
        else:
            if v1 or v2 or v3:
                return True, False
        return False, False

    @staticmethod
    def checkfile(path, start: str="", ext: str="", fstr: str= '', default: bool=True):
        name = os.path.basename(path)
        v, d = myLocalFile.checkname(name, start, ext, fstr, default)
        if not v:
            return False
        elif os.path.isfile(path):
            return True
        else:
            return False

    @staticmethod
    def getchild(path):
        # os.listdir()  方法用于返回指定的文件夹包含的文件或文件夹的名字的列表。这个列表以字母顺序。 它不包括 '.' 和 '..' 即使它在文件夹中。只支持在 Unix, Windows 下使用。
        # http://www.runoob.com/python/os-listdir.html
        aList = []
        path = os.path.expanduser(path)
        isdir = os.path.isdir(path)
        if isdir:
            names = os.listdir(path)
            for aname in names:
                aList.append(os.path.join(path, aname))
        return aList

    @staticmethod
    def checkbranch(path):
        shortname = os.path.basename(path)
        isdir = os.path.isdir(path)
        if isdir and len(shortname) == 10:
            return True
        return False

    @staticmethod
    def checkmonth(path):
        shortname = os.path.basename(path)
        isdir = os.path.isdir(path)
        if isdir and len(shortname) == 6:
            if shortname.isdecimal():
                return int(shortname)
        return -1

    @staticmethod
    def checkday(path):
        shortname = os.path.basename(path)
        isdir = os.path.isdir(path)
        if isdir and len(shortname) == 2:
            if shortname.isdecimal():
                return int(shortname)
        return -1

    @staticmethod
    def unzipTheFile(file, newpath, start: str="", ext: str="", fstr: str= '', default: bool=True):
        # unzip zip file , foldertype = 1 : # 9999900000/201811/01 # 9999900000/20181101
        zip_file = zipfile.ZipFile(file)
        if os.path.isfile(newpath):
            return
        if not os.path.exists(newpath):
            pathlib.Path(newpath).mkdir(parents=True, exist_ok=True)
        for names in zip_file.namelist():
            # if names.lower().startswith(cF.filepre1().lower()):  #'t1_trxrecord'
            v, d = myLocalFile.checkname(names, start, ext, fstr, default)
            if v:
                zip_file.extract(names, newpath)
        zip_file.close()
        return

    @staticmethod
    def convFileLocal(fromF, toFile, needFirstLine=False):
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


class myHdfsFile:
    @staticmethod
    def isdirOrFile(client, path, type=3): # 1 file , 2 dir, 3 any
        theDir = client.status(path, strict=False)
        if theDir is None:
            return False
        else:
            if (type & 1) > 0 and theDir['type'].lower() == 'file':
                return True
            if (type & 2) > 0 and theDir['type'].lower() == 'directory':
                return True
        return False

    @staticmethod
    def isfile(client,path):
        return myHdfsFile.isdirOrFile(client, path, 1)

    @staticmethod
    def isdir(client, path):
        return myHdfsFile.isdirOrFile(client, path, 2)

    @staticmethod
    def checkfile(client, path, start="", ext=""):
        shortname = os.path.basename(path)
        isfile = myHdfsFile.isfile(client, path)
        if isfile:
            if (len(start) == 0 or shortname.lower().startswith(start.lower())) and \
                    (len(ext) == 0 or shortname.lower().endswith(ext.lower())):
                return True
        return False

    @staticmethod
    def getchild(client, path):
        aList = []
        path = os.path.expanduser(path)
        isdir = myHdfsFile.isdir(client, path)
        if isdir:
            names = client.list(path)
            for aname in names:
                aList.append(os.path.join(path, aname))
        return aList

    @staticmethod
    def checkbranch(client, path):
        shortname = os.path.basename(path)
        isdir = myHdfsFile.isdir(client, path)
        if isdir and len(shortname) == 10:
            return True
        return False

    @staticmethod
    def checkmonth(client, path):
        shortname = os.path.basename(path)
        isdir = myHdfsFile.isdir(client, path)
        if isdir and len(shortname) == 6:
            if shortname.isdecimal():
                return int(shortname)
        return -1

    @staticmethod
    def checkday(client, path):
        shortname = os.path.basename(path)
        isdir = myHdfsFile.isdir(client, path)
        if isdir and len(shortname) == 2:
            if shortname.isdecimal():
                return int(shortname)
        return -1

    @staticmethod
    def safeMakedir(client, toFile):
        p = pathlib.Path(toFile).parents
        if type(p) == pathlib._PathParents and len(p._parts) >= 2:
            thePath = p._parts[0]
            for i in range(1, len(p._parts) - 1):
                thePath = os.path.join(thePath, p._parts[i])
                theDir = client.status(thePath, strict=False)
                if theDir is None:
                    client.makedirs(thePath, permission=777)
                #                client.set_owner(thePath,owner='hdfs',group='supergroup')
                else:
                    if theDir['type'].lower() == 'directory':
                        pass
                    else:
                        return
        return

    @staticmethod
    def convFileHDFS(fromF, toFile, client):
        if not os.path.isfile(fromF):
            return
        # newpath = os.path.dirname(toFile)
        # pathlib.Path(newpath).mkdir(parents=True, exist_ok=True)
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
        client.set_permission(toFile, 777)
        f1.close()
        print("ok")

    @staticmethod
    def Test(toFile, client):
        return


# https://www.cnblogs.com/Jims2016/p/8047914.html
# 关于python操作hdfs的API可以查看官网:
# https://hdfscli.readthedocs.io/en/latest/api.html
# https://hdfscli.readthedocs.io/en/latest/api.html#module-hdfs.client
# client.makedirs(hdfs_path)
# client.delete(hdfs_path)
# client.download(hdfs_path, local_path, overwrite=False)
# client.write(hdfs_path, data, overwrite=False, append=True)
# client.write(hdfs_path, data, overwrite=True, append=False)
# client.rename(hdfs_src_path, hdfs_dst_path)
# client.list(hdfs_path, status=False)
# 读取hdfs文件内容,将每行存入数组返回
# def read_hdfs_file(client,filename):
#     lines = []
#     with client.read(filename, encoding='utf-8', delimiter='\n') as reader:
#         for line in reader:
#             # pass
#             # print line.strip()
#             lines.append(line.strip())
#     return lines
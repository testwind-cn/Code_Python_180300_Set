#!coding:utf-8
import sys
import os
import pathlib
import zipfile
import platform


class myLocalFile:
    @staticmethod
    def checkfile(path, start="", ext=""):
        shortname = os.path.basename(path)
        isfile = os.path.isfile(path)
        if isfile:
            if (len(start) == 0 or shortname.lower().startswith(start.lower())) and \
                    (len(ext) == 0 or shortname.lower().endswith(ext.lower())):
                return True
        return False

    @staticmethod
    def getchild(path):
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
    def Test(toFile, client):
        return
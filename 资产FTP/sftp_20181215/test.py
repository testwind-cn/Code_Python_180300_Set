#!/usr/bin/python3
# -*- coding: UTF-8 -*-
from wj_tools import sftp_tool
import sftp_worker
import os.path

if __name__ == "__main__":

    a = sftp_tool.Sftp_Tool(h='', p='', u='', s='', r='', d = 'E:\\git-workspace\\gwind')
    a.getLcFilesList('E:\\git-workspace\\gwind')
    b = sftp_worker.Sftp_Worker("172.31.130.14", 22, "root", "Redhat@2016", "/ftpdata/thblposloan/posflow","C:\\Users\\wangjun\\Desktop\\python\\test")
    b.openSFTP()
    b.downloadFilesByDay(to_dir="", day_str="20181101", from_dir=os.path.join("/ftpdata/thblposloan/posflow", "20181101"))

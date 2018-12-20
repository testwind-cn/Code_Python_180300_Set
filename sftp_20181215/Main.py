#!/usr/bin/python3
# -*- coding: UTF-8 -*-

import sys
from wj_tools.mylog import myLog
from wj_tools.str_tool import StrTool
import sftp_worker
import dataClean

if __name__ == "__main__":

    the_day_str = StrTool.get_param_str(1, "")
    days = StrTool.get_param_int(2, 1)

    myLog.config(filename='sftpLog.log')

    try:
        print('start')
        # scheduler.start()
        worker = sftp_worker.Sftp_Worker(h="172.31.71.71", p=12306, u="yuanxj", s="Uwj1qsFnV8", r="/tmp/", d="/home/data/thzc/")
        isTest = True
        if isTest:
            worker.localDir = "D:/sftp/"
            dataClean.srcPath = "D:/stp_clean/"
            dataClean.aimPath = 'D:/stp_clean/cleanedData/'

        worker.openSFTP()

        worker.downloadFilesByRange(day_str=the_day_str, days=days, from_dir=worker.remoteDir, to_dir=worker.localDir)
        worker.copyFilesByRange(to_dir=dataClean.srcPath, day_str=the_day_str, days=days, file_names=[], from_dir=worker.localDir)

        worker.cleanFilesByRange(day_str=the_day_str, days=days)
        worker.cleanFilePermission()
        #        scanTask()
        print('end')
    except Exception as e:
        myLog.error(str(e))

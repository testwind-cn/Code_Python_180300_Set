#!coding:utf-8
import os
import pathlib
from wj_tools.file_check import MyLocalFile
# data path config file
from conf import ConfigData
import datetime
import shutil


def copyTheFile(destdir, branch, month, day, file, foldertype=1):
    theday = datetime.date(month // 100, month%100, day)
    thedayStr = theday.strftime("%Y%m%d")
    # if month == 201811 and day == 1:
    if thedayStr == ConfigData.test_date():
        pass
    else:
        return

    shortname = os.path.basename(branch)
    if foldertype == 1:
        newpath = os.path.join(destdir, shortname, "{:0>6d}".format(month), "{:0>2d}".format(day))
    else:
        newpath = os.path.join(destdir, "{:0>6d}{:0>2d}".format(month, day), shortname)
    if os.path.isfile(newpath):
        return
    if not os.path.exists(newpath):
        pathlib.Path(newpath).mkdir(parents=True, exist_ok=True)
    toFile = os.path.join(newpath,os.path.basename(file))
    if not os.path.exists(toFile):
        shutil.copyfile(file, toFile)
        print("\nfile copied "+ toFile)


def runCopyFile(conf: ConfigData, isBaoli=True):
    thedate = conf.test_date()      #"20181101"
    root_path = conf.get_zip_path(1)
    destdir = conf.get_data_path(1)
    destdir = os.path.join(destdir, thedate)

    f_name = conf.get_zip_name("*", 1)     # "t1_trxrecord_"  # "_V2.csv"

    print("Start\n")

    branchs = MyLocalFile.get_child(root_path)
    for aBranch in branchs:
        if MyLocalFile.check_branch(aBranch):
            monthes = MyLocalFile.get_child(aBranch)
            for aMonth in monthes:
                theMonth = MyLocalFile.check_month(aMonth)
                if theMonth > 0:
                    days = MyLocalFile.get_child(aMonth)
                    for aDay in days:
                        theDay = MyLocalFile.check_day(aDay)
                        if theDay > 0:
                            files = MyLocalFile.get_child(aDay)
                            for aFile in files:
                                if MyLocalFile.check_file(aFile, p_name=f_name):
                                    copyTheFile(destdir, aBranch, theMonth, theDay, aFile, 1)


if __name__ == "__main__":
    runCopyFile()
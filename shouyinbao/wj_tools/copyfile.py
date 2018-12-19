#!coding:utf-8
import os
import pathlib
from wj_tools.file_check import myLocalFile
# data path config file
from conf import ConfigData as cf
import datetime
import shutil

def copyTheFile(destdir, branch, month, day, file, foldertype=1):
    theday = datetime.date(month // 100, month%100, day)
    thedayStr = theday.strftime("%Y%m%d")
    # if month == 201811 and day == 1:
    if thedayStr == cf.test_date():
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

def runCopyFile(isBaoli=True):
    thedate = cf.test_date()  #"20181101"
    root_path = cf.root_path(isBaoli)
    destdir = cf.unzip_dir(isBaoli)
    destdir = os.path.join(destdir, thedate )

    filepre = cf.file_pre1()  # "t1_trxrecord_"
    fileext = cf.file_ext1()  # "_V2.csv"
    fileext = "_V2.zip"

    print("Start\n")

    branchs = myLocalFile.getchild(root_path)
    for aBranch in branchs:
        if myLocalFile.checkbranch(aBranch):
            monthes = myLocalFile.getchild(aBranch)
            for aMonth in monthes:
                theMonth = myLocalFile.checkmonth(aMonth)
                if theMonth > 0:
                    days = myLocalFile.getchild(aMonth)
                    for aDay in days:
                        theDay = myLocalFile.checkday(aDay)
                        if theDay > 0:
                            files = myLocalFile.getchild(aDay)
                            for aFile in files:
                                if myLocalFile.checkfile(aFile, start=filepre, ext=fileext):
                                    copyTheFile(destdir, aBranch, theMonth, theDay, aFile, 1)


if __name__ == "__main__":
    runCopyFile()
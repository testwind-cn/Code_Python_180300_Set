# -*- coding: utf-8 -*-
import time
import datetime


def getTheDate(thedayStr='',deltaDay=0):
    """ # 19700501 to 21000101

    :param thedayStr: str
    :param deltaDay: int
    :return: date
    """

    if thedayStr is None or type(thedayStr) is not str:
        thedayStr = ''
    try:
        sdate1 = datetime.datetime.strptime(thedayStr, "%Y%m%d").date()
        # stime = time.strptime(the_day_str, "%Y%m%d")
        sdate2 = sdate1 + datetime.timedelta(days=deltaDay)
    except Exception as e:
        sdate1 = datetime.date.today()
        sdate2 = sdate1 + datetime.timedelta(days=deltaDay)
    return sdate2

def getTheDateTick(thedayStr='',deltaDay=0):
    sdate1 = getTheDate(thedayStr=thedayStr, deltaDay=deltaDay)
    thedatetick = time.mktime(sdate1.timetuple())
    return thedatetick

def getTheDateStr(thedayStr='',deltaDay=0):
    sdate1 = getTheDate(thedayStr=thedayStr,deltaDay=deltaDay)
    thedayStr = sdate1.strftime("%Y%m%d")
    #        curYear = 2018
    #        theday = theday.replace(curYear, theday.month, theday.day)
    #        the_day_str = str(theday).replace('-', '')
    return thedayStr


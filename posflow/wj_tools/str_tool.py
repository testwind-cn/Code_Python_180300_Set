import sys
import time
import datetime


class StrTool:

    @staticmethod
    def get_param_int(index: int, default: int = 0):
        np = len(sys.argv)
        if index < np:
            the_str = sys.argv[index]
        else:
            the_str = ""
        if type(the_str) is str and the_str.isdigit():
            return int(the_str)
        else:
            return default

    @staticmethod
    def get_param_str(index: int, default: str = ""):
        np = len(sys.argv)
        if index < np:
            return sys.argv[index]
        else:
            return default

    @staticmethod
    def get_the_date(the_day_str: str ='', delta_day: int =0):
        """ # 19700501 to 21000101

        :param the_day_str: str
        :param delta_day: int
        :return: date
        """

        if the_day_str is None or type(the_day_str) is not str:
            the_day_str = ''
        try:
            sdate1 = datetime.datetime.strptime(the_day_str, "%Y%m%d").date()
            # stime = time.strptime(the_day_str, "%Y%m%d")
            sdate2 = sdate1 + datetime.timedelta(days=delta_day)
        except ValueError as e:
            sdate1 = datetime.date.today()
            sdate2 = sdate1 + datetime.timedelta(days=delta_day)
        return sdate2

    @staticmethod
    def get_the_date_tick(the_day_str: str ='', delta_day: int =0):
        sdate1 = StrTool.get_the_date(the_day_str=the_day_str, delta_day=delta_day)
        thedatetick = time.mktime(sdate1.timetuple())
        return thedatetick

    @staticmethod
    def get_the_date_str(the_day_str: str ='', delta_day: int =0):
        sdate1 = StrTool.get_the_date(the_day_str=the_day_str, delta_day=delta_day)
        the_day_str = sdate1.strftime("%Y%m%d")
        #        curYear = 2018
        #        theday = theday.replace(curYear, theday.month, theday.day)
        #        the_day_str = str(theday).replace('-', '')
        return the_day_str


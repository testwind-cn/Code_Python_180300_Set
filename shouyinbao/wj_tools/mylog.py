# -*- coding: utf-8 -*-
import logging
import traceback


class myLog:

    @staticmethod
    def config(filename,  # 'sftpLog.log'\
               level=logging.INFO, \
               format='%(asctime)s -%(name)s-%(levelname)s-%(module)s:%(message)s', \
               datefmt='%Y-%m-%d %H:%M:%S'):
        logging.basicConfig(filename=filename,
                            format=format,
                            datefmt=datefmt,
                            level=level)
        return

    @staticmethod
    def Log(info='', isInfo=True,needPrint = False):
        if isInfo:
            myLog.info(info,needPrint)
        else:
            myLog.error(info, needPrint)
        return

    @staticmethod
    def info(info='', needPrint=False):
        logging.info(info)
        if needPrint:
            print(info)
        return

    @staticmethod
    def error(error='', needPrint=False):
        logging.error(error)
        if needPrint:
            print(error)
            traceback.print_exc()
        return

#!/usr/bin/python3
# -*- coding: UTF-8 -*-
from wj_tools import  sftp_tool

if __name__ == "__main__":

    a = sftp_tool.Sftp_Tool( h = '172.31.130.14', p = 22, u = 'root', s = 'Redhat@2016', r = '/ftpdata/thblposloan/posflow/', d = 'C:\\Users\\wangjun\\Desktop\\python\\shouyinbao')
    a.openSFTP()
    ll = a.getRmFilesList('/ftpdata/thblposloan/posflow/')
    ll = a.getLcFilesList('C:\\Users\\wangjun\\Desktop\\python\\shouyinbao')
# -*- coding: utf-8 -*-

from wj_tools import sftp_config
import paramiko
import os
import stat


# 获取连接
def getConnect(host, port, username, password):
    """
    :param host: SFTP ip
    :param port: SFTP __m_port
    :param username: SFTP userName
    :param password: SFTP __m_password
    :return: transport
    """
    print("SFTP connection...")
    result = [1, ""]
    try:
        handle = paramiko.Transport((host, port))
        handle.connect(username=username, password=password)
        result = [1, "connection success", handle]
    except Exception as e:
        result = [-1, "connection fail, reason:{0}".format(e)]

    return result


# 关闭连接
def closeConnect(handle: paramiko.Transport):
    result = [1, ""]
    try:
        if isinstance(handle, paramiko.Transport):
            handle.close()
    except Exception as e:
        result = [-1, "close connection fail, reason:{0}".format(e)]

    return result


# 下载文件
def download(handle, remotePath, localAbsDir):
    """
    :param handle:
    :param remotePath: 服务端文件或文件夹的绝对或相对路径
    :param localAbsDir: 客户端文件夹的绝对路径，如E:/SFTP/downDir/
    :return:
    """
    print("start download file by use SFTP...")
    result = [1, ""]
    sftp = paramiko.SFTPClient.from_transport(handle)
    try:
        remotePath = formatPath(remotePath)
        localAbsDir = formatPath(localAbsDir)
        remoteRel = ""
        if remotePath == "":
            remotePath = sftp_config.remoteDir
        else:
            if remotePath.startswith(sftp_config.remoteDir):
                remotePath.replace(sftp_config.remoteDir, "/")
                formatPath(remoteRel)
            else:
                remoteRel = remotePath

        if localAbsDir == "":
            localAbsDir = sftp_config.localDir
            localAbsDir = formatPath(localAbsDir)
        if stat.S_ISREG(sftp.stat(remoteRel).st_mode):  # isFile
            remoteRelPath = remoteRel
            fileName = os.path.basename(remoteRelPath)
            localAbsPath = formatPath(localAbsDir, fileName)
            lad = os.path.split(localAbsPath)[0]
            lad = formatPath(lad)
            if not os.path.exists(lad):
                os.makedirs(lad)
            sftp.get(remoteRelPath, localAbsPath)
            result = [1, "download " + fileName + " success"]
        else:  # isDir
            remoteRelDir = remoteRel
            for pd in sftp.listdir(remoteRelDir):  # pd is dir or file 'name
                rp = formatPath(remoteRelDir, pd)
                if isDir(sftp, rp):
                    lad = formatPath(localAbsDir, pd)
                else:
                    lad = localAbsDir
                rs = download(handle, rp, lad)
                result[1] = result[1] + "\n" + rs[1]
                if rs[0] == -1:
                    result[0] = -1
                else:
                    if result[0] != -1:
                        result[0] = 1
    except Exception as e:
        result = [-1, "download fail, reason:{0}".format(e)]
    sftp.close()
    return result
    # handle.close()


# 上传
def upload(handle, remoteRelDir, localPath):
    """
    :param handle:
    :param remoteRelDir: 服务端文件夹相对路径，可以为None、""，此时文件上传到homeDir
    :param localPath: 客户端文件或文件夹路径，当路径以localDir开始，文件保存到homeDir的相对路径下
    :return:
    """
    print("start upload file by use SFTP...")
    result = [1, ""]
    sftp = paramiko.SFTPClient.from_transport(handle)

    try:
        remoteRelDir = formatPath(remoteRelDir)
        localPath = formatPath(localPath)
        localRelDir = ""
        if localPath == "":
            localPath = sftp_config.localDir
            localPath = formatPath(localPath)
        else:
            if localPath.startswith(sftp_config.localDir):  # 绝对路径
                localRelDir = localPath.replace(sftp_config.localDir, "/")
                localRelDir = formatPath(localRelDir)
            else:  # 相对(__m_localDir)路径
                localPath = formatPath(sftp_config.localDir, localPath)

        if remoteRelDir == "":
            remoteRelDir = formatPath("/uploadFiles/", localRelDir)
        else:
            if remoteRelDir.startswith(sftp_config.remoteDir):
                remoteRelDir = remoteRelDir.replace(sftp_config.remoteDir, "/")
                remoteRelDir = formatPath(remoteRelDir)
        if os.path.isdir(localPath):  # isDir
            rs = uploadDir(sftp, remoteRelDir, localPath)
        else:  # isFile
            rs = uploadFile(sftp, remoteRelDir, localPath)
        if rs[0] == -1:
            result[0] = -1
            result[1] = result[1] + "\n" + rs[1]
    except Exception as e:
        result = [-1, "upload fail, reason:{0}".format(e)]
    sftp.close()
    return result
    # handle.close()


# 上传指定文件夹下的所有
def uploadDir(sftp, remoteRelDir, localAbsDir):
    """
    :param sftp:
    :param remoteRelDir: 服务端文件夹相对路径，可以为None、""，此时文件上传到homeDir
    :param localAbsDir: 客户端文件夹路径，当路径以localDir开始，文件保存到homeDir的相对路径下
	:return:
    """
    print("start upload dir by use SFTP...")
    result = [1, ""]
    try:
        for root, dirs, files in os.walk(localAbsDir):
            if len(files) > 0:
                for fileName in files:
                    localAbsPath = formatPath(localAbsDir, fileName)
                    rs = uploadFile(sftp, remoteRelDir, localAbsPath)
                    if rs[0] == -1:
                        result[0] = -1
                    result[1] = result[1] + "\n" + rs[1]
            if len(dirs) > 0:
                for dirName in dirs:
                    rrd = formatPath(remoteRelDir, dirName)
                    lad = formatPath(localAbsDir, dirName)
                    rs = uploadDir(sftp, rrd, lad)
                    if rs[0] == -1:
                        result[0] = -1
                    result[1] = result[1] + "\n" + rs[1]
            break
    except Exception as e:
        result = [-1, "upload fail, reason:{0}".format(e)]
    return result


# 上传指定文件
def uploadFile(sftp, remoteRelDir, localAbsPath):
    """
	:param sftp:
    :param remoteRelDir: 服务端文件夹相对路径，可以为None、""，此时文件上传到homeDir
    :param localAbsPath: 客户端文件路径，当路径以localDir开始，文件保存到homeDir的相对路径下
    :return:
	"""
    print("start upload file by use SFTP...")
    result = [1, ""]
    try:
        try:
            sftp.chdir(remoteRelDir)
        except:
            try:
                sftp.mkdir(remoteRelDir)
            except:
                print("U have no authority to make dir")
        fileName = os.path.basename(localAbsPath)
        remoteRelPath = formatPath(remoteRelDir, fileName)
        sftp.put(localAbsPath, remoteRelPath)
        result = [1, "upload " + fileName + " success"]
    except Exception as e:
        result = [-1, "upload fail, reason:{0}".format(e)]
    return result


# 判断remote path isDir or isFile
def isDir(sftp, path):
    try:
        sftp.chdir(path)
        return True
    except:
        return False


# return last dir'name in the path, like os.path.basename
def lastDir(path):
    path = formatPath(path)
    paths = path.split("/")
    if len(paths) >= 2:
        return paths[-2]
    else:
        return ""


# 格式化路径或拼接路径并格式化
def formatPath(path, *paths):
    """
    :param path: 路径1
    :param paths: 路径2-n
    :return:
	"""
    if path is None or path == "." or path == "/" or path == "//":
        path = ""
    if len(paths) > 0:
        for pi in paths:
            if pi == "" or pi == ".":
                continue
            path = path + "/" + pi
    if path == "":
        return path
    while path.find("\\") >= 0:
        path = path.replace("\\", "/")
    while path.find("//") >= 0:
        path = path.replace("//", "/")
    if path.find(":/") > 0:  # 含磁盘符 NOT EQ ZERO, os.path.isabs NOT WORK
        if path.startswith("/"):
            path = path[1:]
    else:
        if not path.startswith("/"):
            path = "/" + path
    if os.path.isdir(path):  # remote path is not work
        if not path.endswith("/"):
            path = path + "/"
    elif os.path.isfile(path):  # remote path is not work
        if path.endswith("/"):
            path = path[:-1]
    elif path.find(".") < 0:  # maybe it is a dir
        if not path.endswith("/"):
            path = path + "/"
    else:  # maybe it is a file
        if path.endswith("/"):
            path = path[:-1]

    # print("new path is " + path)
    return path
#!coding:utf-8
# import sys
import os
import fnmatch
import paramiko
import pathlib
import zipfile
# import platform
from wj_tools.mylog import myLog
from hdfs.client import Client


class MyLocalFile:

    @staticmethod
    def safe_make_dir(dir_str: str, mode: int = 0) -> object:
        """stat.S_IRWXO + stat.S_IRWXG + stat.S_IRWXU

        :param dir_str:
        :param mode:
        :return:
        """
        try:
            if not os.path.exists(dir_str):
                # os.makedirs(dir_str)
                pathlib.Path(dir_str).mkdir(parents=True, exist_ok=True)
                if mode > 0:
                    os.chmod(dir_str, mode)
        except Exception as e:
            myLog.Log("safeMakedir Error: " + str(e), False)
        return

    @staticmethod
    def is_exist(path: str, f_type: int=3):  # 1 file , 2 dir, 3 any
        if (f_type & 1) > 0 and os.path.isfile(path):
            return True
        if (f_type & 2) > 0 and os.path.isdir(path):
            return True
        return False

    @staticmethod
    def isfile(path: str):
        return os.path.isfile(path)

    @staticmethod
    def isdir(path: str):
        return os.path.isdir(path)

    @staticmethod
    def check_name(name: str, start: str= "", ext: str= "", fstr: str= '', default: bool=True):
        """

        :param name:
        :param start:
        :param ext:
        :param fstr:
        :param default:
        :return:isIn, isDefault
        """
        if type(name) is not str or len(name) <= 0:
            return default, True

        if type(start) is not str:
            start = ""

        if type(ext) is not str:
            ext = ""

        if type(fstr) is not str:
            fstr = ""

        d1 = (len(start) <= 0)
        d2 = (len(ext) <= 0)
        d3 = (len(fstr) <= 0)

        if d1 and d2 and d3:
            return default, True

        v1 = len(start) > 0 and name.lower().startswith(start.lower())
        v2 = len(ext) > 0 and name.lower().endswith(ext.lower())
        v3 = len(fstr) > 0 and (name.lower().find(fstr.lower()) >= 0)

        if default:
            if (v1 or d1) and (v2 or d2) and (v3 or d3):
                return True, False
        else:
            if v1 or v2 or v3:
                return True, False
        return False, False

    @staticmethod
    def check_file(path: str, start: str= "", ext: str= "", fstr: str= '', default: bool=True):
        name = os.path.basename(path)
        v, d = MyLocalFile.check_name(name, start, ext, fstr, default)
        if not v:
            return False
        elif os.path.isfile(path):
            return True
        else:
            return False

    @staticmethod
    def get_child(path: str, f_type: int=3):  # 1 file , 2 dir, 3 any
        # os.listdir()  方法用于返回指定的文件夹包含的文件或文件夹的名字的列表。这个列表以字母顺序。 它不包括 '.' 和 '..' 即使它在文件夹中。只支持在 Unix, Windows 下使用。
        # http://www.runoob.com/python/os-listdir.html
        a_list = []
        path = os.path.expanduser(path)
        is_dir = os.path.isdir(path)
        if is_dir:
            names = os.listdir(path)
            for a_name in names:
                a_file = os.path.join(path, a_name)
                if f_type == 3:
                    a_list.append(a_file)
                elif MyLocalFile.is_exist(a_file, f_type=f_type):
                    a_list.append(a_file)

        return a_list

    @staticmethod
    def get_child_file(path: str):
        a_list = MyLocalFile.get_child(path, f_type=1)
        return a_list

    @staticmethod
    def get_child_dir(path: str):
        a_list = MyLocalFile.get_child(path, f_type=2)
        return a_list

    @staticmethod
    def check_branch(path: str):
        short_name = os.path.basename(path)
        is_dir = os.path.isdir(path)
        if is_dir and len(short_name) == 10:
            return True
        return False

    @staticmethod
    def check_month(path: str):
        short_name = os.path.basename(path)
        is_dir = os.path.isdir(path)
        if is_dir and len(short_name) == 6:
            if short_name.isdecimal():
                return int(short_name)
        return -1

    @staticmethod
    def check_day(path: str):
        short_name = os.path.basename(path)
        is_dir = os.path.isdir(path)
        if is_dir and len(short_name) == 2:
            if short_name.isdecimal():
                return int(short_name)
        return -1

    @staticmethod
    def unzip_the_file(file: str, newpath: str, start: str= "", ext: str= "", fstr: str= '', default: bool=True):
        # unzip zip file , foldertype = 1 : # 9999900000/201811/01 # 9999900000/20181101
        zip_file = zipfile.ZipFile(file)
        if os.path.isfile(newpath):
            return
        if not os.path.exists(newpath):
            pathlib.Path(newpath).mkdir(parents=True, exist_ok=True)
        for names in zip_file.namelist():
            # if names.lower().startswith(cF.filepre1().lower()):  #'t1_trxrecord'
            v, d = MyLocalFile.check_name(names, start, ext, fstr, default)
            if v:
                zip_file.extract(names, newpath)
        zip_file.close()
        return

    @staticmethod
    def conv_file_local(from_file: str, to_file: str, need_first_line: bool=False):
        if not os.path.isfile(from_file):
            return
        new_path = os.path.dirname(to_file)
        pathlib.Path(new_path).mkdir(parents=True, exist_ok=True)

        f1 = open(from_file, 'r', encoding="gb18030")
        f2 = open(to_file, 'w', encoding="utf-8")
        data = '  '
        i = 0
        while len(data) > 0:
            data = f1.readline()
            #  print(data + "\n\n")
            #  print(i)
            #        code = chardet.detect(dd)['encoding']
            #        print(code)
            #        middle = dd.decode("gb18030")  # gb2312
            #        ss = middle.encode("utf-8")
            #        ss = dd.encode("utf-8")
            #        print(dd)
            #        if i > 0 or platform.platform().lower().startswith("linux"):
            #            f2.write(data)
            if i > 0 or need_first_line:
                f2.write(data)
            # print("\nWrite")
            i = i + 1
        print("Write " + to_file + "\n")
        f1.close()
        f2.close()
        print("ok")


class MyHdfsFile:
    @staticmethod
    def is_exist(client: Client, path: str, f_type: int=3):  # 1 file , 2 dir, 3 any
        the_dir = client.status(path, strict=False)
        if the_dir is None:
            return False
        else:
            if (f_type & 1) > 0 and the_dir['type'].lower() == 'file':
                return True
            if (f_type & 2) > 0 and the_dir['type'].lower() == 'directory':
                return True
        return False

    @staticmethod
    def isfile(client: Client, path: str):
        return MyHdfsFile.is_exist(client, path, 1)

    @staticmethod
    def isdir(client: Client, path: str):
        return MyHdfsFile.is_exist(client, path, 2)

    @staticmethod
    def check_file(client: Client, path: str, start: str="", ext: str=""):
        short_name = pathlib.PurePosixPath(path).name
        is_file = MyHdfsFile.isfile(client, path)
        if is_file:
            if (len(start) == 0 or short_name.lower().startswith(start.lower())) and \
                    (len(ext) == 0 or short_name.lower().endswith(ext.lower())):
                return True
        return False

    @staticmethod
    def get_child(client: Client, path: str, f_type: int=3):  # 1 file , 2 dir, 3 any
        a_list = []
        # path = str(pathlib.PosixPath(path).expanduser())
        is_dir = MyHdfsFile.isdir(client, path)
        if is_dir:
            names = client.list(path)
            for a_name in names:
                a_file = str(pathlib.PurePosixPath(path).joinpath(a_name))
                if f_type == 3:
                    a_list.append(a_file)
                elif MyHdfsFile.is_exist(client, a_file, f_type=f_type):
                    a_list.append(a_file)
        return a_list

    @staticmethod
    def get_child_file(client: Client, path: str):
        a_list = MyHdfsFile.get_child(client, path, f_type=1)
        return a_list

    @staticmethod
    def get_child_dir(client: Client, path: str):
        a_list = MyHdfsFile.get_child(client, path, f_type=2)
        return a_list

    @staticmethod
    def delete(client: Client, path: str, p_name: str):
        # a_list = MyHdfsFile.get_child_file(client, path)
        # for a_name in a_list:
        #     a_file = pathlib.PurePosixPath(a_name).name
        #     if fnmatch.fnmatch(a_file, p_name):
        #         client.delete(a_name, recursive=True)

        # import ssh
        # 新建一个ssh客户端对象
        # ssh_client = ssh.SSHClient()
        # # 设置成默认自动接受密钥
        # ssh_client.set_missing_host_key_policy(ssh.AutoAddPolicy())
        # # 连接远程主机
        # ssh_client.connect("10.91.1.20", port=22, username="root", password="Redhat@2016")
        # # 在远程机执行shell命令
        # stdin, stdout, stderr = client.exec_command("ls -l")
        # # 读返回结果
        # print(stdout.read())
        # del_cmd = "hadoop dfs -rm -r -skipTrash " + str(pathlib.PurePosixPath(path).joinpath(p_name))
        # stdin, stdout, stderr = client.exec_command(del_cmd)
        # print(stdout.read())

        # import paramiko
        ssh_client = paramiko.SSHClient()
        ssh_client.load_system_host_keys()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())  #paramiko.WarningPolicy()
        ssh_client.connect("10.91.1.20", port=22, username="root", password="Redhat@2016")
        # paramiko.SSHClient().exec_command() 可以执行一条命令；当执行多条命令时，多条命令放在一个单引号下面，各命令之间用分号隔开，且在末尾加上get_pty = True。
        # 在command命令最后加上 get_pty=True，执行多条命令 的话用；隔开，另外所有命令都在一个大的单引号范围内引用
        del_cmd = 'hadoop dfs -rm -r -skipTrash ' + str(pathlib.PurePosixPath(path).joinpath(p_name))
        # 'hadoop dfs -ls /user/hive/warehouse/posflow.db/t1_trxrecprd_v2'
        # del_cmd = 'hadoop dfs -ls /user/hive/warehouse/posflow.db/t1_trxrecprd_v2'
        # stdin, stdout, stderr = ssh_client.exec_command(command='su hdfs;'+del_cmd, get_pty=True)
        stdin, stdout, stderr = ssh_client.exec_command(command='su hdfs -c \''+del_cmd+'\'', get_pty=True)
        for line in stdout:
            print(line.strip('\n'))
        ssh_client.close()

        # channel = ssh_client.invoke_shell()
        # stdin = channel.makefile('wb')
        # stdout = channel.makefile('rb')
        # #stdin.write("su hdfs \n"+del_cmd+"\n"+"exit\n")
        # stdin.write("su hdfs \n" + "ls" + "\n" + "exit\nEOF")
        # stdin.close()
        # print(stdout.read())
        # stdout.close()



        # for line in stdout:
        #     print(line.strip('\n'))
        #
        # err = stderr.readlines()
        # if err:
        #     print(err)
        # ssh_client.close()


    @staticmethod
    def check_branch(client: Client, path: str):
        short_name = pathlib.PurePosixPath(path).name
        is_dir = MyHdfsFile.isdir(client, path)
        if is_dir and len(short_name) == 10:
            return True
        return False

    @staticmethod
    def check_month(client: Client, path: str):
        short_name = pathlib.PurePosixPath(path).name
        is_dir = MyHdfsFile.isdir(client, path)
        if is_dir and len(short_name) == 6:
            if short_name.isdecimal():
                return int(short_name)
        return -1

    @staticmethod
    def check_day(client: Client, path: str):
        short_name = pathlib.PurePosixPath(path).name
        is_dir = MyHdfsFile.isdir(client, path)
        if is_dir and len(short_name) == 2:
            if short_name.isdecimal():
                return int(short_name)
        return -1

    @staticmethod
    def safe_make_dir(client: Client, to_file: str):
        p = pathlib.PurePosixPath(to_file)  # pathlib.Path(to_file).parents
        if len(p.parts) >= 2:  # type(p) == pathlib._PathParents and
            the_path = p.parts[0]
            for i in range(1, len(p.parts) - 1):
                the_path = pathlib.PurePosixPath(the_path).joinpath(pathlib.PurePosixPath(p.parts[i]))
                the_path_str = str(the_path)
                # os.path.join(the_path, p.parts[i])
                the_dir = client.status(the_path_str, strict=False)
                if the_dir is None:
                    client.makedirs(the_path_str, permission=777)
                #                client.set_owner(thePath,owner='hdfs',group='supergroup')
                else:
                    if the_dir['type'].lower() == 'directory':
                        pass
                    else:
                        return
        return

    @staticmethod
    def conv_file_hdfs(from_file: str, to_file: str, client: Client):
        if not os.path.isfile(from_file):
            return
        # newpath = os.path.dirname(to_file)
        # pathlib.Path(newpath).mkdir(parents=True, exist_ok=True)
        MyHdfsFile.safe_make_dir(client, to_file)

        f1 = open(from_file, 'r', encoding="gb18030")
        #    f2 = open(to_file, 'w', encoding="utf-8")
        data = '  '
        i = 0
        while len(data) > 0:
            data = f1.readline()
            print(data + "\n\n")
            print(i)
            if i > 0:
                # client.write(hdfs_path, data, overwrite=True, append=False) # first line
                # client.write(hdfs_path, data, overwrite=False, append=True) # rest line
                client.write(to_file, data.encode('utf-8'), overwrite=(i == 1), append=(not (i == 1)))
                print("\nWrite")
            i = i + 1
        client.set_permission(to_file, 777)
        f1.close()
        print("ok")

    @staticmethod
    def test(to_file: str, client: Client):
        return


# https://www.cnblogs.com/Jims2016/p/8047914.html
# 关于python操作hdfs的API可以查看官网:
# https://hdfscli.readthedocs.io/en/latest/api.html
# https://hdfscli.readthedocs.io/en/latest/api.html#module-hdfs.client
# client.makedirs(hdfs_path)
# client.delete(hdfs_path)
# client.download(hdfs_path, local_path, overwrite=False)
# client.write(hdfs_path, data, overwrite=False, append=True)
# client.write(hdfs_path, data, overwrite=True, append=False)
# client.rename(hdfs_src_path, hdfs_dst_path)
# client.list(hdfs_path, status=False)
# 读取hdfs文件内容,将每行存入数组返回
# def read_hdfs_file(client,filename):
#     lines = []
#     with client.read(filename, encoding='utf-8', delimiter='\n') as reader:
#         for line in reader:
#             # pass
#             # print line.strip()
#             lines.append(line.strip())
#     return lines

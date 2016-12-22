# encoding: utf-8
"""
文件工具类
@author Yuriseus
@create 2016-10-8 14:34
"""
import os


class FileUtil(object):
    @staticmethod
    def get_files(dir_path, exclude_ext=None, filter_fun=None):
        """
        获取目录下所有文件
        :param dir_path:
        :param exclude_ext: 排除的文件扩展名数组
        :param filter_fun: 过滤方法，返回True的需要被过滤掉
        :return:
        """
        files = []
        for base, dir_names, file_names in os.walk(dir_path):
            for file_name in file_names:
                shot_name, extension = os.path.splitext(file_name)
                if exclude_ext and extension in exclude_ext:
                    continue
                file_path = os.path.join(base, file_name)
                if filter_fun:
                    if not filter_fun(file_path):
                        files.append(file_path)
                else:
                    files.append(file_path)
        return files

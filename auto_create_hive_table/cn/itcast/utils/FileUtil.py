#!/usr/bin/env python
# @desc : todo 实现读取表名文件
__coding__ = "utf-8"
__author__ = "pku_2024_bigData"


def readFileContent(fileName):
    """
    加载表名所在的文件
    :param fileName:存有表名的文件路径
    :return:存有所有表名的列表集合
    """
    # 定义一个空的列表，用于存放表名，最后返回
    tableNameList = []
    # 打开一个文件
    fr = open(fileName)
    # 遍历每一行
    for line in fr.readlines():
        # 将每一行尾部的换行删掉
        curLine = line.rstrip('\n')
        # 把表名放入列表
        tableNameList.append(curLine)
    # 返回所有表名的列表
    return tableNameList

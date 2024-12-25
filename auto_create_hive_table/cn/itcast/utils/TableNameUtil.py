#!/usr/bin/env python
# @desc : todo 用户将所有表名进行划分，构建全量列表和增量列表
__coding__ = "utf-8"
__author__ = "pku_2024_bigData"


def getODSTableNameList(fileNameList):
    """
    基于传递的所有表名，将增量表与全量表进行划分到不同的列表中
    :param fileNameList: 所有表名的列表
    :return: 增量与全量列表
    """
    # 定义全量空列表
    full_list = []
    # 定义增量空列表
    incr_list = []
    # 用于返回的结果列表
    result_list = []
    # 定义一个bool值，默认为true
    isFull = True
    # 取出集合中的每一个表名
    for line in fileNameList:
        # 如果isFull = True
        if isFull:
            # 如果当前取到的表名为@
            if "@".__eq__(line):
                # 将 isFull = False
                isFull = False
                # 跳过本次循环
                continue
            # 将表名放入全量表的列表中
            full_list.append(line)
        # 如果isFull = False
        else:
            # 将表名放入增量列表
            incr_list.append(line)
    # 将全量列表和增量列表放入一个结果列表中
    result_list.append(full_list)
    result_list.append(incr_list)
    # 返回结果列表
    return result_list
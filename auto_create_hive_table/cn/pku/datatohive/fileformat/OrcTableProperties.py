#!/usr/bin/env python
# @Time : 2021/7/12 16:01
# @desc : hdfs存储数据为orc格式
__coding__ = "utf-8"
__author__ = "pku_2024_bigData"

from auto_create_hive_table.cn.pku.datatohive.fileformat.TableProperties import TableProperties


class OrcTableProperties(TableProperties):
    def getStoreFmtAndProperties(self, tableName):
        return "stored as orc\n"

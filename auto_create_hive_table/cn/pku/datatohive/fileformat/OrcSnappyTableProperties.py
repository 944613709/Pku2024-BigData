#!/usr/bin/env python
# @Time : 2024/7/12 16:00
# @desc : hdfs存储数据为orc格式并设置snappy压缩
__coding__ = "utf-8"
__author__ = "pku_2024_bigData"

from auto_create_hive_table.cn.pku.datatohive.fileformat.OrcTableProperties import OrcTableProperties


class OrcSnappyTableProperties(OrcTableProperties):
    def getStoreFmtAndProperties(self, tableName):
        return super(OrcSnappyTableProperties, self).getStoreFmtAndProperties(
            tableName) + "tblproperties (\"orc.compress\"=\"SNAPPY\")\n"

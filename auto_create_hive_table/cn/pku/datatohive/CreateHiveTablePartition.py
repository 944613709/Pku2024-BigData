#!/usr/bin/env python
# @desc : TODO 根据动态表，动态创建hive分区
__coding__ = "utf-8"
__author__ = "pku_2024_bigData"

from auto_create_hive_table.cn.pku.datatohive import CreateMetaCommon
import logging


class CreateHiveTablePartition(object):

    def __init__(self, hiveConn):
        self.hiveConn = hiveConn

    def executeCPartition(self, dbName, hiveTName, dynamicDir, partitionDT):
        """
        用于实现给Hive表的数据手动申明分区
        :param dbName: 数据库名称
        :param hiveTName: 表名称
        :param dynamicDir: 全量或者增量
        :param partitionDT: 分区值
        :return: None
        """
        # 构建空的列表，拼接SQL语句
        buffer = []
        # 定义一个游标
        cursor = None
        try:
            # SQL拼接：alter table one_make_ods.
            buffer.append("alter table " + dbName + ".")
            # SQL拼接：表名
            buffer.append(hiveTName)
            # SQL拼接：add if not exists partition (dt='
            buffer.append(" add if not exists partition (dt='")
            # SQL拼接：20210101
            buffer.append(partitionDT)
            # SQL拼接：') location 'data/dw/ods/one_make/full_imp/ciss4.'
            buffer.append("') location '/data/dw/" + CreateMetaCommon.getDBFolderName(dbName) +
                          "/one_make/" + CreateMetaCommon.getDynamicDir(dbName, dynamicDir) + "/ciss4.")
            # SQL拼接：表名
            buffer.append(hiveTName)
            # SQL拼接：/
            buffer.append("/")
            # SQL拼接：分区目录
            buffer.append(partitionDT)
            buffer.append("'")
            # 实例化SparkSQL游标
            cursor = self.hiveConn.cursor()
            # 执行SQL语句
            cursor.execute(''.join(buffer))
            # 输出日志
            logging.warning(f'执行创建hive\t{hiveTName}表的分区：{partitionDT},\t分区sql:\n{"".join(buffer)}')
        # 异常处理
        except Exception as e:
            print(e)
        # 释放游标
        finally:
            if cursor:
                cursor.close()

#!/usr/bin/env python
# @desc : todo 将ODS层表的数据导入到DWD层
__coding__ = "utf-8"
__author__ = "pku_2024_bigData"

from typing import List
import logging
from auto_create_hive_table.cn.pku.datatohive import CreateMetaCommon
from auto_create_hive_table.cn.pku.utils import OracleHiveUtil

class LoadData2DWD:
    """
    加载数据到DWD层的处理类
    主要处理从ODS层到DWD层的数据加工
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.hive_conn = OracleHiveUtil.getSparkHiveConn()
        self.cursor = self.hive_conn.cursor()

    def execute_hql(self, hql: str) -> bool:
        """
        执行HQL语句
        :param hql: HQL语句
        :return: 是否执行成功
        """
        try:
            self.cursor.execute(hql)
            return True
        except Exception as e:
            self.logger.error(f"Failed to execute HQL: {str(e)}")
            return False

    @staticmethod
    def loadTable(oracle_conn, hive_conn, table_name: str, partition_val: str) -> bool:
        """
        从ODS层加载数据到DWD层
        :param oracle_conn: Oracle连接
        :param hive_conn: Hive连接
        :param table_name: 表名
        :param partition_val: 分区值
        :return: 是否加载成功
        """
        try:
            cursor = hive_conn.cursor()
            
            # 首先获取目标表的列信息
            desc_sql = f"DESC {CreateMetaCommon.DWD_NAME}.{table_name}"
            cursor.execute(desc_sql)
            columns = []
            for row in cursor.fetchall():
                col_name = row[0]
                if col_name != 'dt':  # 排除分区列
                    columns.append(col_name)
            
            # 构建DWD层数据加载SQL
            columns_str = ", ".join([f"t.{col}" for col in columns])
            hql = f"""
            INSERT OVERWRITE TABLE {CreateMetaCommon.DWD_NAME}.{table_name}
            PARTITION(dt='{partition_val}')
            SELECT 
                {columns_str}
            FROM {CreateMetaCommon.ODS_NAME}.{table_name} t
            WHERE dt = '{partition_val}'
            """
            
            # 执行HQL
            cursor.execute(hql)
            return True
            
        except Exception as e:
            logging.error(f"Failed to load table {table_name} to DWD: {str(e)}")
            return False

    def load_data(self, date_str: str) -> bool:
        """
        加载指定日期的数据到DWD层
        :param date_str: 数据日期，格式：YYYYMMDD
        :return: 是否加载成功
        """
        try:
            # 构建DWD层数据加载SQL
            hql = f"""
            -- 加载工单事实表
            INSERT OVERWRITE TABLE one_make_dwd.fact_worker_order
            PARTITION(dt='{date_str}')
            SELECT 
                wo_id,
                wo_num,
                wo_type,
                wo_status,
                install_num,
                repair_num,
                remould_num,
                inspection_num,
                alread_complete_num,
                oil_station_id
            FROM one_make_ods.fact_worker_order
            WHERE dt = '{date_str}';

            -- 加载呼叫服务事实表
            INSERT OVERWRITE TABLE one_make_dwd.fact_call_service
            PARTITION(dt='{date_str}')
            SELECT 
                call_id,
                process_way_name,
                call_type,
                call_status,
                oil_station_id
            FROM one_make_ods.fact_call_service
            WHERE dt = '{date_str}';
            """
            
            # 执行HQL
            if self.execute_hql(hql):
                self.logger.info(f"Successfully loaded data to DWD for date: {date_str}")
                return True
            return False
            
        except Exception as e:
            self.logger.error(f"Failed to load data to DWD: {str(e)}")
            return False

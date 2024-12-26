from typing import List
import logging
from auto_create_hive_table.cn.pku.datatohive import CreateMetaCommon
from auto_create_hive_table.cn.pku.utils import OracleHiveUtil

class LoadData2DM:
    """
    加载数据到DM层（数据集市）的处理类
    主要处理面向应用的聚合统计数据
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

    def load_worker_order_summary(self, date_str: str) -> bool:
        """
        加载工单汇总数据到DM层
        :param date_str: 数据日期，格式：YYYYMMDD
        :return: 处理是否成功
        """
        try:
            # 构建DM层工单汇总SQL
            hql = f"""
            INSERT OVERWRITE TABLE one_make_dm.dm_worker_order_summary
            PARTITION(dt='{date_str}')
            SELECT 
                dws_month,
                dws_week,
                oil_province,
                oil_type,
                customer_classify,
                sum(wokerorder_num) as total_orders,
                sum(install_sumnum) as total_installs,
                sum(repair_sumnum) as total_repairs,
                sum(remould_sumnum) as total_remoulds,
                sum(inspection_sumnum) as total_inspections,
                sum(alread_complete_num) as total_completed,
                avg(wokerorder_num_avg) as avg_orders,
                max(wokerorder_num_max) as max_orders,
                min(wokerorder_num_min) as min_orders
            FROM one_make_dws.dws_worker_order_stats
            WHERE dt = '{date_str}'
            GROUP BY 
                dws_month,
                dws_week,
                oil_province,
                oil_type,
                customer_classify
            """
            
            # 执行HQL
            if self.execute_hql(hql):
                self.logger.info(f"Successfully loaded worker order summary to DM for date: {date_str}")
                return True
            return False
            
        except Exception as e:
            self.logger.error(f"Failed to load worker order summary to DM: {str(e)}")
            return False

    def load_customer_distribution(self, date_str: str) -> bool:
        """
        加载客户分布数据到DM层
        :param date_str: 数据日期，格式：YYYYMMDD
        :return: 处理是否成功
        """
        try:
            # 构建客户分布统计SQL
            hql = f"""
            INSERT OVERWRITE TABLE one_make_dm.dm_customer_distribution
            PARTITION(dt='{date_str}')
            SELECT 
                province_name,
                city_name,
                customer_classify_name,
                sum(customer_count) as total_customers,
                count(distinct city_name) as city_coverage,
                '{date_str}' as stat_date
            FROM one_make_dws.dws_customer_classify_stats
            WHERE dt = '{date_str}'
            GROUP BY 
                province_name,
                city_name,
                customer_classify_name
            """
            
            # 执行HQL
            if self.execute_hql(hql):
                self.logger.info(f"Successfully loaded customer distribution to DM for date: {date_str}")
                return True
            return False
            
        except Exception as e:
            self.logger.error(f"Failed to load customer distribution to DM: {str(e)}")
            return False 
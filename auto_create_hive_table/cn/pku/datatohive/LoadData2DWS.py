from typing import List
import logging
from auto_create_hive_table.cn.pku.datatohive import CreateMetaCommon
from auto_create_hive_table.cn.pku.utils import OracleHiveUtil

class LoadData2DWS:
    """
    加载数据到DWS层的处理类
    主要处理工单统计、客户分类统计等服务层面的数据聚合
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.hive_conn = OracleHiveUtil.getSparkHiveConn()

    def execute_hql(self, hql: str) -> bool:
        """
        执行HQL语句
        :param hql: HQL语句
        :return: 是否执行成功
        """
        try:
            self.hive_conn.execute(hql)
            return True
        except Exception as e:
            self.logger.error(f"Failed to execute HQL: {str(e)}")
            return False

    def load_worker_order_stats(self, date_str: str) -> bool:
        """
        加载工单统计数据到DWS层
        :param date_str: 数据日期，格式：YYYYMMDD
        :return: 处理是否成功
        """
        try:
            # 构建DWS层工单统计SQL
            hql = f"""
            INSERT OVERWRITE TABLE one_make_dws.dws_worker_order_stats 
            PARTITION(dt='{date_str}')
            SELECT 
                max(owner_process_num) as owner_process,
                max(tran_process_num) as tran_process,
                sum(wo.wo_num) as wokerorder_num,
                max(wo.wo_num) as wokerorder_num_max,
                min(wo.wo_num) as wokerorder_num_min,
                avg(wo.wo_num) as wokerorder_num_avg,
                sum(install_num) as install_sumnum,
                sum(repair_num) as repair_sumnum,
                sum(remould_num) as remould_sumnum,
                sum(inspection_num) as inspection_sumnum,
                sum(alread_complete_num) as alread_complete_sumnum,
                dd.date_id as dws_day,
                dd.week_in_year_id as dws_week,
                dd.year_month_id as dws_month,
                oil.company_name as oil_type,
                oil.province_name as oil_province,
                oil.city_name as oil_city,
                oil.county_name as oil_county,
                oil.customer_classify_name as customer_classify,
                oil.customer_province_name as customer_province
            FROM one_make_dwb.fact_worker_order wo
            LEFT JOIN one_make_dws.dim_date dd ON wo.dt = dd.date_id
            LEFT JOIN one_make_dws.dim_oilstation oil ON wo.oil_station_id = oil.id
            WHERE dd.date_id = '{date_str}'
            GROUP BY 
                dd.date_id, dd.week_in_year_id, dd.year_month_id,
                oil.company_name, oil.province_name, oil.city_name, oil.county_name,
                oil.customer_classify_name, oil.customer_province_name
            """
            
            # 执行HQL
            if self.execute_hql(hql):
                self.logger.info(f"Successfully loaded worker order stats to DWS for date: {date_str}")
                return True
            return False
            
        except Exception as e:
            self.logger.error(f"Failed to load worker order stats to DWS: {str(e)}")
            return False

    def load_customer_classify_stats(self, date_str: str) -> bool:
        """
        加载客户分类统计数据到DWS层
        :param date_str: 数据日期，格式：YYYYMMDD
        :return: 处理是否成功
        """
        try:
            # 构建客户分类统计SQL
            hql = f"""
            INSERT OVERWRITE TABLE one_make_dws.dws_customer_classify_stats
            PARTITION(dt='{date_str}')
            SELECT 
                customer_classify_name,
                count(1) as customer_count,
                province_name,
                city_name,
                '{date_str}' as stat_date
            FROM one_make_dws.dim_oilstation
            GROUP BY 
                customer_classify_name,
                province_name,
                city_name
            """
            
            # 执行HQL
            if self.execute_hql(hql):
                self.logger.info(f"Successfully loaded customer classification stats to DWS for date: {date_str}")
                return True
            return False
            
        except Exception as e:
            self.logger.error(f"Failed to load customer classification stats to DWS: {str(e)}")
            return False 
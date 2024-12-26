from typing import List
import logging
from auto_create_hive_table.cn.pku.datatohive import CreateMetaCommon
from auto_create_hive_table.cn.pku.utils import OracleHiveUtil

class LoadData2ST:
    """
    加载数据到ST层（服务主题层）的处理类
    主要处理工单主题的统计分析
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

    def execute_query(self, hql: str) -> List:
        """
        执行HQL查询语句
        :param hql: HQL查询语句
        :return: 查询结果列表
        """
        try:
            self.cursor.execute(hql)
            return self.cursor.fetchall()
        except Exception as e:
            self.logger.error(f"Failed to execute query: {str(e)}")
            return []

    def load_worker_order_subject(self, date_str: str, month_str: str, week_str: str) -> bool:
        """
        加载工单主题数据到ST层
        :param date_str: 数据日期，格式：YYYYMMDD
        :param month_str: 月份，格式：YYYYMM
        :param week_str: 周，格式：YYYYWn
        :return: 处理是否成功
        """
        try:
            # 构建ST层工单主题统计SQL
            hql = f"""
            INSERT OVERWRITE TABLE one_make_st.subj_worker_order 
            PARTITION(month='{month_str}', week='{week_str}', day='{date_str}')
            SELECT 
                max(owner_process_num) owner_process,
                max(tran_process_num) tran_process,
                sum(fwo.wo_num) wokerorder_num,
                max(fwo.wo_num) wokerorder_num_max,
                min(fwo.wo_num) wokerorder_num_min,
                avg(fwo.wo_num) wokerorder_num_avg,
                sum(install_num) install_sumnum,
                sum(repair_num) repair_sumnum,
                sum(remould_num) remould_sumnum,
                sum(inspection_num) inspection_sumnum,
                sum(alread_complete_num) alread_complete_sumnum,
                max(customerzsh.customerClassify) customer_classify_zsh,
                max(customerjxs.customerClassify) customer_classify_jxs,
                max(customerothersale.customerClassify) customer_classify_qtzx,
                max(customerzsy.customerClassify) customer_classify_zsy,
                max(customerothercnt.customerClassify) customer_classify_qtwlh,
                max(customerozhonghua.customerClassify) customer_classify_zhjt,
                max(customerzhonghy.customerClassify) customer_classify_zhy,
                max(customersupplier.customerClassify) customer_classify_gys,
                max(customeronemake.customerClassify) customer_classify_onemake,
                max(customerseremp.customerClassify) customer_classify_fwy,
                max(customerzhongt.customerClassify) customer_classify_zt,
                max(customercomamy.customerClassify) customer_classify_hzgs,
                max(customerarmi.customerClassify) customer_classify_jg,
                max(customerzhy.customerClassify) customer_classify_zhhangy,
                dd.date_id dws_day,
                dd.week_in_year_id dws_week,
                dd.year_month_id dws_month,
                oil.company_name oil_type,
                oil.province_name oil_province,
                oil.city_name oil_city,
                oil.county_name oil_county,
                oil.customer_classify_name customer_classify,
                oil.customer_province_name customer_province
            FROM one_make_dwb.fact_worker_order fwo,
                (select count(1) owner_process_num from one_make_dwb.fact_call_service fcs where fcs.process_way_name = '自己处理') ownerProcess,
                (select count(1) tran_process_num from one_make_dwb.fact_call_service fcs where fcs.process_way_name = '转派工') tranWork,
                (select count(1) customerClassify from one_make_dws.dim_oilstation oil where oil.customer_classify_name ='中石化') customerzsh,
                (select count(1) customerClassify from one_make_dws.dim_oilstation oil where oil.customer_classify_name ='经销商') customerjxs,
                (select count(1) customerClassify from one_make_dws.dim_oilstation oil where oil.customer_classify_name ='其他直销') customerothersale,
                (select count(1) customerClassify from one_make_dws.dim_oilstation oil where oil.customer_classify_name ='中石油') customerzsy,
                (select count(1) customerClassify from one_make_dws.dim_oilstation oil where oil.customer_classify_name ='其他往来户') customerothercnt,
                (select count(1) customerClassify from one_make_dws.dim_oilstation oil where oil.customer_classify_name ='中化集团') customerozhonghua,
                (select count(1) customerClassify from one_make_dws.dim_oilstation oil where oil.customer_classify_name ='中海油') customerzhonghy,
                (select count(1) customerClassify from one_make_dws.dim_oilstation oil where oil.customer_classify_name ='供应商') customersupplier,
                (select count(1) customerClassify from one_make_dws.dim_oilstation oil where oil.customer_classify_name ='一站制造**') customeronemake,
                (select count(1) customerClassify from one_make_dws.dim_oilstation oil where oil.customer_classify_name ='服务员') customerseremp,
                (select count(1) customerClassify from one_make_dws.dim_oilstation oil where oil.customer_classify_name ='中铁') customerzhongt,
                (select count(1) customerClassify from one_make_dws.dim_oilstation oil where oil.customer_classify_name ='合资公司') customercomamy,
                (select count(1) customerClassify from one_make_dws.dim_oilstation oil where oil.customer_classify_name ='军供') customerarmi,
                (select count(1) customerClassify from one_make_dws.dim_oilstation oil where oil.customer_classify_name ='中航油') customerzhy
            LEFT JOIN one_make_dws.dim_date dd ON fwo.dt = dd.date_id
            LEFT JOIN one_make_dws.dim_oilstation oil ON fwo.oil_station_id = oil.id
            WHERE dd.date_id = '{date_str}'
            GROUP BY 
                dd.date_id, dd.week_in_year_id, dd.year_month_id,
                oil.company_name, oil.province_name, oil.city_name, oil.county_name,
                oil.customer_classify_name, oil.customer_province_name
            """
            
            # 执行HQL
            if self.execute_hql(hql):
                self.logger.info(f"Successfully loaded worker order subject to ST for date: {date_str}")
                return True
            return False
            
        except Exception as e:
            self.logger.error(f"Failed to load worker order subject to ST: {str(e)}")
            return False

    def get_partition_info(self, date_str: str) -> tuple:
        """
        根据日期获取分区信息
        :param date_str: 数据日期，格式：YYYYMMDD
        :return: (month_str, week_str)
        """
        try:
            # 查询分区信息
            hql = f"""
            SELECT DISTINCT
                year_month_id,
                week_in_year_id
            FROM one_make_dws.dim_date
            WHERE date_id = '{date_str}'
            """
            result = self.execute_query(hql)
            if result and len(result) > 0:
                return result[0][0], result[0][1]  # month_str, week_str
            else:
                raise Exception(f"No partition info found for date: {date_str}")
            
        except Exception as e:
            self.logger.error(f"Failed to get partition info: {str(e)}")
            raise 
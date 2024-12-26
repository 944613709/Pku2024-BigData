#!/usr/bin/env python
# @desc : todo ODS&DWD建库、建表、装载数据主类
__coding__ = "utf-8"
__author__ = "pku_2024_bigData"

# 导入读Oracle表、建Hive表的包
from auto_create_hive_table.cn.pku.datatohive import CHiveTableFromOracleTable, CreateMetaCommon, CreateHiveTablePartition, LoadData2DWD
# 导入工具类：连接Oracle工具类、文件工具类、表名构建工具类
from auto_create_hive_table.cn.pku.utils import OracleHiveUtil, FileUtil, TableNameUtil
# 导入日志工具包
from auto_create_hive_table.config import common
import logging
from auto_create_hive_table.cn.pku.datatohive.LoadData2DWD import LoadData2DWD
from auto_create_hive_table.cn.pku.datatohive.LoadData2DWS import LoadData2DWS
from auto_create_hive_table.cn.pku.datatohive.LoadData2DM import LoadData2DM
from auto_create_hive_table.cn.pku.datatohive.LoadData2ST import LoadData2ST

# 根据不同功能接口记录不同的日志
admin_logger = common.get_logger('pku')


def recordLog(modelName):
    """
    记录普通级别日志：Debug、Info、WARN、Error
    :param modelName: 模块名称
    :return: 日志信息
    """
    msg = f'{modelName}'
    admin_logger.info(msg)
    return msg


def recordWarnLog(msg):
    """
    记录警告级别日志
    :param msg: 日志信息
    :return: 日志信息
    """
    admin_logger.warning(msg)
    return msg


class EntranceApp:
    """
    数据处理入口类
    """
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.dwd_loader = LoadData2DWD()
        self.dws_loader = LoadData2DWS()
        self.dm_loader = LoadData2DM()
        self.st_loader = LoadData2ST()

    def process_data(self, date_str: str) -> bool:
        """
        处理数据的主入口方法
        :param date_str: 数据日期，格式：YYYYMMDD
        :return: 处理是否成功
        """
        try:
            # 1. DWD层数据处理
            self.logger.info("Starting DWD layer processing...")
            if not self.dwd_loader.load_data(date_str):
                self.logger.error("Failed to process DWD layer")
                return False

            # 2. DWS层数据处理
            self.logger.info("Starting DWS layer processing...")
            if not self.dws_loader.load_worker_order_stats(date_str):
                self.logger.error("Failed to process worker order stats in DWS layer")
                return False
            
            if not self.dws_loader.load_customer_classify_stats(date_str):
                self.logger.error("Failed to process customer classification stats in DWS layer")
                return False

            # 3. DM层数据处理
            self.logger.info("Starting DM layer processing...")
            if not self.dm_loader.load_worker_order_summary(date_str):
                self.logger.error("Failed to process worker order summary in DM layer")
                return False
            
            if not self.dm_loader.load_customer_distribution(date_str):
                self.logger.error("Failed to process customer distribution in DM layer")
                return False

            # 4. ST层数据处理
            self.logger.info("Starting ST layer processing...")
            try:
                # 获取分区信息
                month_str, week_str = self.st_loader.get_partition_info(date_str)
                # 加载工单主题数据
                if not self.st_loader.load_worker_order_subject(date_str, month_str, week_str):
                    self.logger.error("Failed to process worker order subject in ST layer")
                    return False
            except Exception as e:
                self.logger.error(f"Failed to process ST layer: {str(e)}")
                return False

            self.logger.info("All data processing completed successfully")
            return True

        except Exception as e:
            self.logger.error(f"Error in data processing: {str(e)}")
            return False


if __name__ == '__main__':

    # =================================1-初始化Oracle、Hive连接，读取表的名称=========================#
    # 输出信息
    recordLog('ODS&DWD Building AND Load Data')
    # 定义了一个分区变量：指定当前要操作的Hive分区的值为20240101
    partitionVal = '20240102'
    # 调用了获取连接的工具类，构建一个Oracle连接
    oracleConn = OracleHiveUtil.getOracleConn()
    # 调用了获取连接的工具类，构建一个SparkSQL连接
    hiveConn = OracleHiveUtil.getSparkHiveConn()
    # 调用了文件工具类读取表名所在的文件：将所有表的名称放入一个列表：List[102个String类型的表名]
    tableList = FileUtil.readFileContent(
        r"D:\mycoding\python\bigData_pku2024\OneMake30\dw\ods\meta_data\tablenames.txt")
    # 调用工具类，将全量表的表名存入一个列表，将增量表的表名存入另外一个列表中，再将这两个列表放入一个列表中：List[2个List元素：List1[44张全量表的表名]，List2[57张增量表的表名]]
    tableNameList = TableNameUtil.getODSTableNameList(tableList)

    # =================================2-ODS层建库=============================================#
    cHiveTableFromOracleTable = CHiveTableFromOracleTable(oracleConn, hiveConn)
    recordLog('ODS层创建数据库')
    cHiveTableFromOracleTable.executeCreateDbHQL(CreateMetaCommon.ODS_NAME)

    # =================================3-ODS层建表=============================================#
    recordLog('ODS层创建全量表...')
    fullTableList = tableNameList[0]
    recordLog('ODS层创建增量表...')
    incrTableList = tableNameList[1]

    # =================================4-ODS层申明分区=============================================#
    recordLog('创建ods层全量表分区...')
    createHiveTablePartition = CreateHiveTablePartition(hiveConn)
    recordLog('创建ods层增量表分区...')

    # =================================5-DWD层建库建表=============================================#
    recordLog('DWD层创建数据库')
    cHiveTableFromOracleTable.executeCreateDbHQL(CreateMetaCommon.DWD_NAME)

    recordLog('DWD层创建表...')
    allTableName = [i for j in tableNameList for i in j]

    # =================================6-DWD层数据抽取=============================================#
    recordWarnLog('DWD层加载数据，此操作将启动Spark JOB执行，请稍后...')
    for tblName in allTableName:
        recordLog(f'加载dwd层数据到{tblName}表...')
        try:
            LoadData2DWD.loadTable(oracleConn, hiveConn, tblName, partitionVal)
        except Exception as error:
            print(error)
        recordLog('完成!!!')

    # =================================7-DWS层数据处理=============================================#
    recordWarnLog('DWS层加载数据，此操作将启动Spark JOB执行，请稍后...')
    try:
        dws_loader = LoadData2DWS()
        recordLog('加载DWS层工单统计数据...')
        if not dws_loader.load_worker_order_stats(partitionVal):
            recordWarnLog('DWS层工单统计数据加载失败')
        recordLog('完成!!!')
        
        recordLog('加载DWS层客户分类统计数据...')
        if not dws_loader.load_customer_classify_stats(partitionVal):
            recordWarnLog('DWS层客户分类统计数据加载失败')
        recordLog('完成!!!')
    except Exception as error:
        print(error)

    # =================================8-DM层数据处理=============================================#
    recordWarnLog('DM层加载数据，此操作将启动Spark JOB执行，请稍后...')
    try:
        dm_loader = LoadData2DM()
        recordLog('加载DM层工单汇总数据...')
        if not dm_loader.load_worker_order_summary(partitionVal):
            recordWarnLog('DM层工单汇总数据加载失败')
        recordLog('完成!!!')
        
        recordLog('加载DM层客户分布数据...')
        if not dm_loader.load_customer_distribution(partitionVal):
            recordWarnLog('DM层客户分布数据加载失败')
        recordLog('完成!!!')
    except Exception as error:
        print(error)

    # =================================9-ST层数据处理=============================================#
    recordWarnLog('ST层加载数据，此操作将启动Spark JOB执行，请稍后...')
    try:
        st_loader = LoadData2ST()
        recordLog('加载ST层工单主题数据...')
        # 获取分区信息
        month_str, week_str = st_loader.get_partition_info(partitionVal)
        if not st_loader.load_worker_order_subject(partitionVal, month_str, week_str):
            recordWarnLog('ST层工单主题数据加载失败')
        recordLog('完成!!!')
    except Exception as error:
        print(error)

    # =================================10-程序结束，释放资源=============================================#
    oracleConn.close()
    hiveConn.close()

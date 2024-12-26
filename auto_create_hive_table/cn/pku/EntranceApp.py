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
from cn.pku.datatohive.LoadData2DWD import LoadData2DWD
from cn.pku.datatohive.LoadData2DWS import LoadData2DWS
from cn.pku.datatohive.LoadData2DM import LoadData2DM
from cn.pku.datatohive.LoadData2ST import LoadData2ST

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

    # =================================todo: 1-初始化Oracle、Hive连接，读取表的名称=========================#
    # 输出信息
    recordLog('ODS&DWD Building AND Load Data')
    # 定义了一个分区变量：指定当前要操作的Hive分区的值为20240101
    partitionVal = '20240101'
    # 调用了获取连接的工具类，构建一个Oracle连接
    oracleConn = OracleHiveUtil.getOracleConn()
    # 调用了获取连接的工具类，构建一个SparkSQL连接
    hiveConn = OracleHiveUtil.getSparkHiveConn()
    # 调用了文件工具类读取表名所在的文件：将所有表的名称放入一个列表：List[102个String类型的表名]
    tableList = FileUtil.readFileContent("D:\\PythonProject\\OneMake30\\dw\\ods\\meta_data\\tablenames.txt")
    # 调用工具类，将全量表的表名存入一个列表，将增量表的表名存入另外一个列表中，再将这两个列表放入一个列表中：List[2个List元素：List1[44张全量表的表名]，List2[57张增量表的表���]]
    tableNameList = TableNameUtil.getODSTableNameList(tableList)
    # ------------------测试：输出获取到的连接以及所有表名
    # print(oracleConn)
    # print(hiveConn)
    # for tbnames in tableNameList:
    #     print("---------------------")
    #     for tbname in tbnames:
    #         print(tbname)

    # =================================todo: 2-ODS层建库=============================================#
    # 构建了一个建库建表的类的对象：实例化的时候给连接赋值
    cHiveTableFromOracleTable = CHiveTableFromOracleTable(oracleConn, hiveConn)
    # 打印日志
    recordLog('ODS层创建数据库')
    # 调用这个类的创建数据库的方法：传递ODS层数据库的名称
    cHiveTableFromOracleTable.executeCreateDbHQL(CreateMetaCommon.ODS_NAME)


    # =================================todo: 3-ODS层建表=============================================#
    # 打印日志
    recordLog('ODS层创建全量表...')
    # 从表名的列表中取出第一个元素：全量表名的列表
    fullTableList = tableNameList[0]
    # 取出每张全量表的表名
    # for tblName in fullTableList:
    #     # 创建全量表：ODS层数据库名称，全量表的表名，full_imp
    #     cHiveTableFromOracleTable.executeCreateTableHQL(CreateMetaCommon.ODS_NAME, tblName, CreateMetaCommon.FULL_IMP)
    # 打印日志
    recordLog('ODS层创建增量表...')
    # # 从表名的列表中取出第二个元素：增量表名的列表
    incrTableList = tableNameList[1]
    # 取出每张增量表的表名
    # for tblName in incrTableList:
    #     # Hive中创建这张增量表：ODS层数据库名称,增量表的表名，incr_imp
    #     cHiveTableFromOracleTable.executeCreateTableHQL(CreateMetaCommon.ODS_NAME, tblName, CreateMetaCommon.INCR_IMP)


    # =================================todo: 4-ODS层申明分区=============================================#
    recordLog('创建ods层全量表分区...')
    # 构建专门用于申明分区的类的对象
    createHiveTablePartition = CreateHiveTablePartition(hiveConn)
    # 全量表执行44次创建分区操作
    # for tblName in fullTableList:
    #     # 调用申明分区的方法申明全量表的分区：ods层数据库名称、表名、full_imp,20210101
    #     createHiveTablePartition.executeCPartition(CreateMetaCommon.ODS_NAME, tblName, CreateMetaCommon.FULL_IMP, partitionVal)

    recordLog('创建ods层增量表分区...')
    # 增量表执行57次创建分区操作
    # for tblName in incrTableList:
    #     createHiveTablePartition.executeCPartition(CreateMetaCommon.ODS_NAME, tblName, CreateMetaCommon.INCR_IMP, partitionVal)


    # =================================todo: 5-DWD层建库建表=============================================#
    # 5.1 建库记录日志
    recordLog('DWD层创建数据库')
    # 创建DWD层数据库
    cHiveTableFromOracleTable.executeCreateDbHQL(CreateMetaCommon.DWD_NAME)

    # 5.2 建表记录日志
    recordLog('DWD层创建表...')
    # 将所有表名合并到一个列表中
    allTableName = [i for j in tableNameList for i in j]
    # 取出每张表名
    # for tblName in allTableName:
    #     # 实现DWD层建表：数据库one_make_dwd,表名,
    #     cHiveTableFromOracleTable.executeCreateTableHQL(CreateMetaCommon.DWD_NAME, tblName, None)

    # =================================todo: 6-DWD层数据抽取=============================================#
    # 记录日志
    recordWarnLog('DWD层加载数据，此操作将启动Spark JOB执行，请稍后...')
    # 取出每张表的表名
    for tblName in allTableName:
        recordLog(f'加载dwd层数据到{tblName}表...')
        try:
            # 从ODS层抽取数据到DWD层：oracle连接、Hive连接、表名、分区的值：20210101
            LoadData2DWD.loadTable(oracleConn, hiveConn, tblName, partitionVal)
        except Exception as error:
            print(error)
        recordLog('完成!!!')

# =================================todo: 7-程序结束，释放资源=============================================#
oracleConn.close()
hiveConn.close()

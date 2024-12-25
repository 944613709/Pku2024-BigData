#!/usr/bin/env python
# @desc : todo 实现构建Oracle、Hive、SparkSQL的连接
__coding__ = "utf-8"
__author__ = "pku_2024_bigData"

# 导包
from auto_create_hive_table.cn.pku.utils import ConfigLoader         # 导入配置文件解析包
import cx_Oracle                                                        # 导入Python连接Oracle依赖库包
from pyhive import hive                                                 # 导入Python连接Hive依赖包
import os                                                               # 导入系统包

# 配置Oracle的客户端驱动文件路径
LOCATION = r"D:\\instantclient_12_2"
os.environ["PATH"] = LOCATION + ";" + os.environ["PATH"]


def getOracleConn():
    """
    用户获取Oracle的连接对象：cx_Oracle.connect(host='', port='', username='', password='', param='')
    :return:
    """
    oracleConn = None   #构建Oracle连接对象
    try:
        ORACLE_HOST = ConfigLoader.getOracleConfig('oracleHost')                # 获取Oracle连接的主机地址
        ORACLE_PORT = ConfigLoader.getOracleConfig('oraclePort')                # 获取Oracle连接的端口
        ORACLE_SID = ConfigLoader.getOracleConfig('oracleSID')                  # 获取Oracle连接的SID
        ORACLE_USER = ConfigLoader.getOracleConfig('oracleUName')               # 获取Oracle连接的用户名
        ORACLE_PASSWORD = ConfigLoader.getOracleConfig('oraclePassWord')        # 获取Oracle连接的密码
        # 构建DSN
        dsn = cx_Oracle.makedsn(ORACLE_HOST, ORACLE_PORT, ORACLE_SID)
        # 获取真正的Oracle连接
        oracleConn = cx_Oracle.connect(ORACLE_USER, ORACLE_PASSWORD, dsn)
    # 异常处理
    except cx_Oracle.Error as error:
        print(error)
    # 返回Oracle连接
    return oracleConn


def getSparkHiveConn():
    """
    用户获取SparkSQL的连接对象
    :return:
    """
    # 构建SparkSQL的连接对象
    sparkHiveConn = None
    try:
        SPARK_HIVE_HOST = ConfigLoader.getSparkConnHiveConfig('sparkHiveHost')              # 获取Spark连接的主机地址
        SPARK_HIVE_PORT = ConfigLoader.getSparkConnHiveConfig('sparkHivePort')              # 获取Spark连接的端口
        SPARK_HIVE_UNAME = ConfigLoader.getSparkConnHiveConfig('sparkHiveUName')            # 获取Spark连接的用户名
        SPARK_HIVE_PASSWORD = ConfigLoader.getSparkConnHiveConfig('sparkHivePassWord')      # 获取Spark连接的密码
        # 获取一个Spark TriftServer连接对象
        sparkHiveConn = hive.Connection(host=SPARK_HIVE_HOST, port=SPARK_HIVE_PORT, username=SPARK_HIVE_UNAME, auth='CUSTOM', password=SPARK_HIVE_PASSWORD)
    # 异常处理
    except Exception as error:
        print(error)
    # 返回连接对象
    return sparkHiveConn


def getHiveConn():
    """
    用户获取HiveServer2的连接对象
    :return:
    """
    # 构建Hive的连接对象
    hiveConn = None
    try:
        HIVE_HOST= ConfigLoader.getHiveConfig("hiveHost")           # 获取Hive连接的主机地址
        HIVE_PORT= ConfigLoader.getHiveConfig("hivePort")           # 获取Hive连接的端口
        HIVE_USER= ConfigLoader.getHiveConfig("hiveUName")          # 获取Hive连接的用户名
        HIVE_PASSWORD= ConfigLoader.getHiveConfig("hivePassWord")   # 获取Hive连接的密码
        # 构建一个Hive的连接对象
        hiveConn = hive.Connection(host=HIVE_HOST,port=HIVE_PORT,username=HIVE_USER,auth='CUSTOM',password=HIVE_PASSWORD)
    # 异常处理
    except Exception as error:
        print(error)
    # 返回Hive连接对象
    return hiveConn


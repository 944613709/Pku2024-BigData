#!/usr/bin/env python
# @desc : todo 加载并读取数据库连接信息工具类
__coding__ = "utf-8"
__author__ = "pku_2024_bigData"

import configparser
import os

# 获取当前文件所在目录的绝对路径
current_dir = os.path.dirname(os.path.abspath(__file__))
# 构建到resources/config.txt的相对路径
config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(current_dir))), 'resources', 'config.txt')

# load and read config.ini
config = configparser.ConfigParser()
config.read(config_path)

# 根据key获得value
def getProperty(section, key):
    return config.get(section, key)

# 根据key获得oracle数据库连接的配置信息
def getOracleConfig(key):
    return config.get('OracleConn', key)

# 根据key获得spark连接hive数据库的配置信息
def getSparkConnHiveConfig(key):
    return config.get('SparkConnHive', key)

# 根据key获得hive数据库的配置信息
def getHiveConfig(key):
    return config.get('HiveConn', key)

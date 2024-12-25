from pyhive import hive
from typing import List, Dict, Any, Optional
import pandas as pd


class HiveConnector:
    def __init__(self, host: str = 'localhost', port: int = 10000, 
                 username: Optional[str] = None, database: str = 'default'):
        """
        初始化Hive连接器
        
        Args:
            host: Hive服务器地址
            port: Hive端口
            username: 用户名
            database: 默认数据库
        """
        self.host = host
        self.port = port
        self.username = username
        self.database = database
        self.conn = None
        self.cursor = None

    def connect(self) -> None:
        """建立Hive连接"""
        try:
            self.conn = hive.Connection(
                host=self.host,
                port=self.port,
                username=self.username,
                database=self.database
            )
            self.cursor = self.conn.cursor()
            print(f"Successfully connected to Hive server at {self.host}:{self.port}")
        except Exception as e:
            print(f"Failed to connect to Hive: {str(e)}")
            raise

    def disconnect(self) -> None:
        """关闭Hive连接"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            print("Disconnected from Hive server")

    def execute_query(self, query: str) -> List[tuple]:
        """
        执行HiveQL查询
        
        Args:
            query: HiveQL查询语句
            
        Returns:
            查询结果列表
        """
        try:
            if not self.conn or not self.cursor:
                self.connect()
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            print(f"Query execution failed: {str(e)}")
            raise

    def execute_query_pandas(self, query: str) -> pd.DataFrame:
        """
        执行查询并返回Pandas DataFrame
        
        Args:
            query: HiveQL查询语句
            
        Returns:
            包含查询结果的DataFrame
        """
        try:
            results = self.execute_query(query)
            if results and self.cursor.description:
                columns = [desc[0] for desc in self.cursor.description]
                return pd.DataFrame(results, columns=columns)
            return pd.DataFrame()
        except Exception as e:
            print(f"Failed to convert results to DataFrame: {str(e)}")
            raise

    def get_tables(self, database: Optional[str] = None) -> List[str]:
        """
        获取指定数据库中的所有表
        
        Args:
            database: 数据库名称，如果为None则使用当前数据库
            
        Returns:
            表名列表
        """
        db = database or self.database
        query = f"SHOW TABLES IN {db}"
        results = self.execute_query(query)
        return [row[0] for row in results]

    def get_table_schema(self, table: str, database: Optional[str] = None) -> List[Dict[str, str]]:
        """
        获取表结构
        
        Args:
            table: 表名
            database: 数据库名称，如果为None则使用当前数据库
            
        Returns:
            包含字段信息的字典列表
        """
        db = database or self.database
        query = f"DESCRIBE {db}.{table}"
        results = self.execute_query(query)
        schema = []
        for row in results:
            if row[0] and not row[0].startswith('#'):
                schema.append({
                    'column_name': row[0],
                    'data_type': row[1],
                    'comment': row[2] if len(row) > 2 else ''
                })
        return schema 

if __name__ == "__main__":
    hive_connector = HiveConnector(host='node1', port=10000, username='root', database='one_make_st')
    hive_connector.connect()
    
    # 首先列出数据库中的所有表
    print("Available tables:")
    tables = hive_connector.get_tables()
    print(tables)
    
    # 如果user_info表存在，再查看其schema
    if 'subj_dispatch' in tables:
        print("\nSchema for subj_dispatch:")
        print(hive_connector.get_table_schema('subj_dispatch'))
    else:
        print("\nTable 'subj_dispatch' does not exist in database 'one_make_st'")
    
    hive_connector.disconnect()

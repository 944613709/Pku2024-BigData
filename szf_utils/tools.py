from typing import List, Dict, Any, Optional
from .hive_connector import HiveConnector


class HiveTools:
    def __init__(self, hive_connector: HiveConnector):
        """
        初始化Hive工具类
        
        Args:
            hive_connector: HiveConnector实例
        """
        self.hive = hive_connector

    def query_data(self, query: str) -> str:
        """
        执行查询并返回格式化结果
        
        Args:
            query: HiveQL查询语句
            
        Returns:
            格式化的查询结果字符串
        """
        try:
            df = self.hive.execute_query_pandas(query)
            return df.to_string()
        except Exception as e:
            return f"查询执行失败: {str(e)}"

    def list_tables(self, database: Optional[str] = None) -> str:
        """
        列出数据库中的所有表
        
        Args:
            database: 数据库名称
            
        Returns:
            表名列表的字符串表示
        """
        try:
            tables = self.hive.get_tables(database)
            return "\n".join(tables)
        except Exception as e:
            return f"获取表列表失败: {str(e)}"

    def describe_table(self, table: str, database: Optional[str] = None) -> str:
        """
        获取表结构的描述
        
        Args:
            table: 表名
            database: 数据库名称
            
        Returns:
            表结构的字符串描述
        """
        try:
            schema = self.hive.get_table_schema(table, database)
            result = []
            for field in schema:
                result.append(f"字段名: {field['column_name']}")
                result.append(f"类型: {field['data_type']}")
                if field['comment']:
                    result.append(f"注释: {field['comment']}")
                result.append("-" * 30)
            return "\n".join(result)
        except Exception as e:
            return f"获取表结构失败: {str(e)}"

    def get_available_tools(self) -> List[Dict[str, str]]:
        """
        获取所有可用工具的描述
        
        Returns:
            工具描述列表
        """
        return [
            {
                "name": "query_data",
                "description": "执行HiveQL查询并返回结果",
                "parameters": "query: str - HiveQL查询���句"
            },
            {
                "name": "list_tables",
                "description": "列出指定数据库中的所有表",
                "parameters": "database: Optional[str] - 数据库名称（可选）"
            },
            {
                "name": "describe_table",
                "description": "获取表的详细结构信息",
                "parameters": "table: str - 表名, database: Optional[str] - 数据库名称（可选）"
            }
        ] 
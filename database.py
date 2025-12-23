import pandas as pd
from sqlalchemy import create_engine, text, inspect
import re
import logging
from typing import List, Dict, Any, Optional, Tuple
from config import DATABASE_CONFIG, SYSTEM_CONFIG

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self):
        self.engine = self._create_mysql_engine()
        self._test_connection()
        logger.info("✅ MySQL数据库连接成功")

    def _create_mysql_engine(self):
        """创建MySQL数据库引擎"""
        try:
            username = DATABASE_CONFIG['username']
            password = DATABASE_CONFIG['password']
            host = DATABASE_CONFIG['host']
            port = DATABASE_CONFIG['port']
            database = DATABASE_CONFIG['database']
            driver = DATABASE_CONFIG['driver']

            connection_string = f"mysql+{driver}://{username}:{password}@{host}:{port}/{database}?charset=utf8mb4"

            logger.info(f"连接MySQL: {host}:{port}/{database}")

            engine = create_engine(
                connection_string,
                pool_pre_ping=True,
                pool_recycle=3600
            )

            return engine

        except Exception as e:
            logger.error(f"创建MySQL连接失败: {e}")
            raise

    def _test_connection(self):
        """测试数据库连接"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                logger.info("✅ MySQL连接测试成功")
        except Exception as e:
            logger.error(f"❌ MySQL连接测试失败: {e}")
            raise

    def get_tables(self):
        """获取所有表名"""
        try:
            inspector = inspect(self.engine)
            tables = inspector.get_table_names()
            logger.info(f"发现 {len(tables)} 个表")
            return tables
        except Exception as e:
            logger.error(f"获取表列表错误: {e}")
            return self._get_tables_by_query()

    def _get_tables_by_query(self):
        """通过SQL查询获取表名"""
        try:
            query = "SHOW TABLES"
            result_df = self.execute_query(query)
            if not result_df.empty and 'error' not in result_df.columns:
                tables = result_df.iloc[:, 0].tolist()
                logger.info(f"通过SHOW TABLES发现 {len(tables)} 个表")
                return tables
            return []
        except Exception as e:
            logger.error(f"通过SHOW TABLES获取表名也失败: {e}")
            return []

    def execute_query(self, query, params=None):
        """执行SQL查询"""
        try:
            with self.engine.connect() as conn:
                if params:
                    result = conn.execute(text(query), params)
                else:
                    result = conn.execute(text(query))

                if result.returns_rows:
                    columns = result.keys()
                    rows = result.fetchall()
                    df = pd.DataFrame(rows, columns=columns)
                    logger.info(f"查询执行成功，返回 {len(df)} 行，{len(columns)} 列")
                    return df
                else:
                    conn.commit()
                    logger.info("非查询语句执行成功")
                    return pd.DataFrame({"message": ["Query executed successfully"]})
        except Exception as e:
            error_msg = f"执行查询时出错: {str(e)}"
            logger.error(error_msg)
            return pd.DataFrame({"error": [error_msg]})

    def get_table_data(self, table_name, limit=50, offset=0):
        """获取表数据"""
        try:
            safe_table_name = f"`{table_name}`"

            if offset > 0:
                query = f"SELECT * FROM {safe_table_name} LIMIT {limit} OFFSET {offset}"
            else:
                query = f"SELECT * FROM {safe_table_name} LIMIT {limit}"

            logger.info(f"执行查询: {query}")
            result = self.execute_query(query)

            if not result.empty and 'error' in result.columns:
                logger.error(f"查询失败: {result['error'].iloc[0]}")
            else:
                logger.info(f"成功获取表数据: {table_name}, 行数: {len(result)}")

            return result

        except Exception as e:
            error_msg = f"获取表数据失败: {str(e)}"
            logger.error(error_msg)
            return pd.DataFrame({"error": [error_msg]})

    def get_table_count(self, table_name):
        """获取表的总行数"""
        try:
            safe_table_name = f"`{table_name}`"
            query = f"SELECT COUNT(*) as total_count FROM {safe_table_name}"
            result = self.execute_query(query)

            if not result.empty and 'total_count' in result.columns:
                count = result['total_count'].iloc[0]
                logger.info(f"表 {table_name} 总行数: {count}")
                return count
            return 0
        except Exception as e:
            logger.error(f"获取表行数失败: {e}")
            return 0

    def get_table_info(self, table_name):
        """获取表的完整信息"""
        try:
            schema = self.get_table_schema(table_name)
            sample_data = self.get_table_data(table_name, limit=50)
            total_count = self.get_table_count(table_name)

            info = {
                'table_name': table_name,
                'schema': schema,
                'sample_data': sample_data,
                'total_count': total_count,
                'sample_count': len(sample_data) if not sample_data.empty else 0,
                'has_error': 'error' in sample_data.columns if not sample_data.empty else False
            }

            if 'error' in sample_data.columns and len(sample_data) == 1:
                info['error'] = sample_data['error'].iloc[0]

            return info
        except Exception as e:
            logger.error(f"获取表信息错误: {e}")
            return {"error": str(e)}

    def get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """获取表结构"""
        try:
            inspector = inspect(self.engine)
            columns = inspector.get_columns(table_name)

            if columns:
                schema_info = []
                for column in columns:
                    column_info = {
                        'name': column['name'],
                        'type': str(column['type']),
                        'nullable': column['nullable'],
                        'primary_key': column.get('primary_key', False),
                        'default': column.get('default', None),
                        'comment': column.get('comment', ''),
                        'autoincrement': column.get('autoincrement', False)
                    }

                    col_type = str(column['type']).lower()
                    if 'int' in col_type:
                        column_info['category'] = 'integer'
                    elif 'float' in col_type or 'decimal' in col_type or 'double' in col_type:
                        column_info['category'] = 'numeric'
                    elif 'char' in col_type or 'text' in col_type:
                        column_info['category'] = 'text'
                    elif 'date' in col_type or 'time' in col_type:
                        column_info['category'] = 'datetime'
                    elif 'bool' in col_type:
                        column_info['category'] = 'boolean'
                    else:
                        column_info['category'] = 'other'

                    schema_info.append(column_info)

                logger.info(f"获取表结构成功: {table_name}, 列数: {len(schema_info)}")
                return schema_info
            else:
                return self._get_table_schema_by_describe(table_name)

        except Exception as e:
            logger.error(f"获取表结构错误: {e}")
            return self._get_table_schema_by_describe(table_name)

    def _get_table_schema_by_describe(self, table_name):
        """使用DESCRIBE查询获取表结构"""
        try:
            query = f"DESCRIBE `{table_name}`"
            result_df = self.execute_query(query)

            if not result_df.empty and 'error' not in result_df.columns:
                schema_info = []
                for _, row in result_df.iterrows():
                    schema_info.append({
                        'name': row['Field'],
                        'type': row['Type'],
                        'nullable': row['Null'] == 'YES',
                        'primary_key': row['Key'] == 'PRI'
                    })
                logger.info(f"通过DESCRIBE获取表结构成功: {table_name}")
                return schema_info
            return []
        except Exception as e:
            logger.error(f"通过DESCRIBE获取表结构也失败: {e}")
            return []

    def test_connection(self):
        """测试数据库连接"""
        try:
            tables = self.get_tables()
            table_list = ", ".join(tables) if tables else "无表"
            return True, f"连接成功，发现 {len(tables)} 个表: {table_list}"
        except Exception as e:
            return False, f"连接失败: {str(e)}"

    def get_simple_query(self, table_name: str) -> str:
        """生成简单的查询语句"""
        schema = self.get_table_schema(table_name)

        columns = []
        for col in schema[:5]:
            columns.append(f"`{col['name']}`")

        if not columns:
            return f"SELECT * FROM `{table_name}` LIMIT 50"

        select_clause = ', '.join(columns)
        return f"SELECT {select_clause} FROM `{table_name}` LIMIT 50"

    def execute_safe_query(self, sql_query: str) -> pd.DataFrame:
        """安全执行SQL查询，自动降级"""
        try:
            result = self.execute_query(sql_query)

            if not result.empty and 'error' in result.columns and len(result) == 1:
                error_msg = result['error'].iloc[0]

                if "Unknown column" in error_msg:
                    logger.info("检测到列名错误，尝试简化查询")

                    table_match = re.search(r'FROM\s+`?([\w]+)`?', sql_query, re.IGNORECASE)
                    if table_match:
                        table_name = table_match.group(1)
                        simple_sql = self.get_simple_query(table_name)
                        logger.info(f"降级到简单查询: {simple_sql}")
                        return self.execute_query(simple_sql)

            return result

        except Exception as e:
            logger.error(f"安全查询执行失败: {e}")
            return pd.DataFrame({"error": [str(e)]})
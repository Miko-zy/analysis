# database.py
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

    def test_connection(self) -> Tuple[bool, str]:
        """测试数据库连接并返回结果"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT VERSION()"))
                version = result.fetchone()[0]
                return True, f"MySQL版本: {version}"
        except Exception as e:
            return False, str(e)

    def get_tables(self) -> List[str]:
        """获取所有表名"""
        try:
            inspector = inspect(self.engine)
            tables = inspector.get_table_names()
            logger.info(f"获取到 {len(tables)} 个表")
            return tables
        except Exception as e:
            logger.error(f"获取表列表失败: {e}")
            return []

    def get_table_schema(self, table_name: str) -> Dict[str, str]:
        """获取表结构信息"""
        try:
            inspector = inspect(self.engine)
            columns = inspector.get_columns(table_name)

            schema = {}
            for col in columns:
                schema[col['name']] = str(col['type'])

            logger.info(f"获取表 {table_name} 的结构信息，共 {len(schema)} 个字段")
            return schema
        except Exception as e:
            logger.error(f"获取表 {table_name} 结构失败: {e}")
            return {}

    def execute_query(self, sql_query: str) -> pd.DataFrame:
        """执行SQL查询并返回DataFrame"""
        try:
            logger.info(f"执行SQL查询: {sql_query}")

            # 限制返回行数
            if 'LIMIT' not in sql_query.upper():
                # 检查是否已经有ORDER BY
                if 'ORDER BY' in sql_query.upper():
                    sql_query = re.sub(
                        r'(ORDER BY.*?)(?=;|$)',
                        r'\1 LIMIT ' + str(SYSTEM_CONFIG['max_rows']),
                        sql_query,
                        flags=re.IGNORECASE | re.DOTALL
                    )
                else:
                    sql_query += f" LIMIT {SYSTEM_CONFIG['max_rows']}"

            df = pd.read_sql(sql_query, self.engine)
            logger.info(f"查询成功，返回 {len(df)} 行数据")
            return df

        except Exception as e:
            logger.error(f"执行查询失败: {e}")
            error_df = pd.DataFrame({'error': [str(e)]})
            return error_df

    def get_table_data(self, table_name: str, limit: int = 50) -> pd.DataFrame:
        """获取表的前N行数据"""
        try:
            sql_query = f"SELECT * FROM `{table_name}` LIMIT {limit}"
            df = pd.read_sql(sql_query, self.engine)
            logger.info(f"获取表 {table_name} 的前 {len(df)} 行数据")
            return df
        except Exception as e:
            logger.error(f"获取表 {table_name} 数据失败: {e}")
            error_df = pd.DataFrame({'error': [str(e)]})
            return error_df
import re
from typing import List, Dict, Any, Optional, Tuple


class SQLHelper:
    @staticmethod
    def extract_tables_from_sql(sql_query: str) -> List[str]:
        """从SQL中提取表名"""
        tables = []

        # 查找FROM子句中的表名
        from_match = re.search(r'FROM\s+([\w`]+)', sql_query, re.IGNORECASE)
        if from_match:
            tables.append(from_match.group(1).replace('`', ''))

        # 查找JOIN子句中的表名
        join_matches = re.findall(r'JOIN\s+([\w`]+)', sql_query, re.IGNORECASE)
        for match in join_matches:
            tables.append(match.replace('`', ''))

        return list(set(tables))

    @staticmethod
    def extract_columns_from_sql(sql_query: str) -> List[str]:
        """从SQL中提取列名"""
        columns = []

        # 查找SELECT子句中的列
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql_query, re.IGNORECASE | re.DOTALL)
        if select_match:
            select_clause = select_match.group(1)
            # 简单的列名提取（不支持复杂表达式）
            col_matches = re.findall(r'`([\w]+)`', select_clause)
            columns.extend(col_matches)

        return columns

    @staticmethod
    def validate_table_exists(sql_query: str, available_tables: List[str]) -> Tuple[bool, str]:
        """验证SQL中使用的表是否存在"""
        used_tables = SQLHelper.extract_tables_from_sql(sql_query)

        for table in used_tables:
            if table not in available_tables:
                return False, f"表 '{table}' 不存在"

        return True, "所有表都存在"

    @staticmethod
    def add_limit_if_missing(sql_query: str, default_limit: int = 100) -> str:
        """如果SQL没有LIMIT，添加LIMIT子句"""
        if 'LIMIT' not in sql_query.upper():
            # 查找ORDER BY的位置
            order_by_match = re.search(r'ORDER BY.*?$', sql_query, re.IGNORECASE)
            if order_by_match:
                # 在ORDER BY之后添加LIMIT
                sql_query = sql_query[:order_by_match.end()] + f' LIMIT {default_limit}' + sql_query[
                                                                                           order_by_match.end():]
            else:
                # 在末尾添加LIMIT
                sql_query += f' LIMIT {default_limit}'

        return sql_query

    @staticmethod
    def simplify_sql(sql_query: str) -> str:
        """简化SQL查询"""
        # 移除不必要的空格和换行
        sql_query = re.sub(r'\s+', ' ', sql_query).strip()

        # 移除多余的括号
        sql_query = re.sub(r'\(\s*\)', '', sql_query)

        return sql_query

    @staticmethod
    def generate_simple_query(table_name: str, columns: List[Dict], limit: int = 10) -> str:
        """生成简单的SQL查询"""
        if not columns:
            return f"SELECT * FROM `{table_name}` LIMIT {limit}"

        # 选择前3个字段
        selected_cols = []
        for col in columns[:3]:
            col_name = col['name']
            # 添加别名（如果字段名是英文，可以尝试翻译）
            if re.match(r'^[a-zA-Z_]+$', col_name):
                alias = SQLHelper._infer_column_alias(col_name)
                selected_cols.append(f"`{col_name}` as `{alias}`")
            else:
                selected_cols.append(f"`{col_name}`")

        select_clause = ', '.join(selected_cols)
        return f"SELECT {select_clause} FROM `{table_name}` LIMIT {limit}"

    @staticmethod
    def _infer_column_alias(column_name: str) -> str:
        """推断列名的中文别名"""
        col_lower = column_name.lower()

        alias_map = {
            'id': 'ID',
            'name': '名称',
            'title': '标题',
            'date': '日期',
            'time': '时间',
            'amount': '金额',
            'price': '价格',
            'cost': '成本',
            'count': '数量',
            'quantity': '数量',
            'status': '状态',
            'type': '类型',
            'category': '分类',
            'user': '用户',
            'customer': '客户',
            'product': '产品',
            'order': '订单',
            'total': '总计',
            'average': '平均',
            'max': '最大值',
            'min': '最小值'
        }

        for key, value in alias_map.items():
            if key in col_lower:
                return value

        # 如果没有匹配，返回原始名称
        return column_name
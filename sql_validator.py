# sql_validator.py
import re
from typing import List, Dict, Any, Optional, Tuple, Set
import difflib
import logging

logger = logging.getLogger(__name__)


class SQLValidator:
    """SQL验证和修正工具"""

    def __init__(self):
        self.keyword_patterns = {
            'aggregation': ['COUNT', 'SUM', 'AVG', 'MAX', 'MIN', 'GROUP_CONCAT'],
            'clauses': ['SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'HAVING', 'LIMIT', 'JOIN', 'LEFT JOIN',
                        'INNER JOIN', 'RIGHT JOIN'],
            'operators': ['AND', 'OR', 'NOT', 'IN', 'BETWEEN', 'LIKE', 'IS NULL', 'IS NOT NULL']
        }

    def extract_columns_from_sql(self, sql: str) -> List[str]:
        """从SQL中提取列名"""
        columns = []

        # 移除字符串字面量
        sql_no_strings = re.sub(r"'(.*?)'", "'STRING'", sql)
        sql_no_strings = re.sub(r'"(.*?)"', "'STRING'", sql_no_strings)

        # 查找反引号包裹的列名
        backtick_matches = re.findall(r'`([^`]+)`', sql_no_strings)
        columns.extend(backtick_matches)

        # 查找AS别名后的列名（简单模式）
        as_matches = re.findall(r'AS\s+["\']?([a-zA-Z0-9_\u4e00-\u9fa5]+)["\']?', sql_no_strings, re.IGNORECASE)
        columns.extend(as_matches)

        return list(set(columns))  # 去重

    def extract_tables_from_sql(self, sql: str) -> List[str]:
        """从SQL中提取表名"""
        tables = []

        # 移除字符串字面量
        sql_no_strings = re.sub(r"'(.*?)'", "'STRING'", sql)
        sql_no_strings = re.sub(r'"(.*?)"', "'STRING'", sql_no_strings)

        # 查找FROM后的表名
        from_matches = re.findall(r'FROM\s+[`"]?([a-zA-Z0-9_]+)[`"]?', sql_no_strings, re.IGNORECASE)
        tables.extend(from_matches)

        # 查找JOIN后的表名
        join_matches = re.findall(r'(?:LEFT\s+|RIGHT\s+|INNER\s+)?JOIN\s+[`"]?([a-zA-Z0-9_]+)[`"]?', sql_no_strings, re.IGNORECASE)
        tables.extend(join_matches)

        return list(set(tables))  # 去重

    def fix_column_names(self, sql: str, available_columns: List[str], table_name: str) -> Tuple[str, List[str]]:
        """修正SQL中的列名"""
        if not sql or not available_columns:
            return sql, []

        corrections = []
        fixed_sql = sql

        # 提取SQL中的列名
        sql_columns = self.extract_columns_from_sql(sql)

        # 为每个SQL列名寻找最佳匹配
        for sql_col in sql_columns:
            if sql_col in available_columns:
                continue  # 列名已经正确

            # 寻找最相似的列名
            best_match = difflib.get_close_matches(sql_col, available_columns, n=1, cutoff=0.6)
            if best_match:
                old_pattern = rf'(?<!\w)`{re.escape(sql_col)}`(?!\w)'
                new_pattern = f'`{best_match[0]}`'
                fixed_sql = re.sub(old_pattern, new_pattern, fixed_sql)
                corrections.append(f"{sql_col} -> {best_match[0]}")
                logger.info(f"修正列名: {sql_col} -> {best_match[0]}")

        return fixed_sql, corrections

    def validate_syntax(self, sql: str) -> Tuple[bool, str]:
        """基础语法验证"""
        if not sql or not isinstance(sql, str):
            return False, "SQL为空"

        sql_upper = sql.upper().strip()

        # 检查是否以SELECT开头
        if not sql_upper.startswith('SELECT'):
            return False, "SQL必须以SELECT开头"

        # 检查危险操作
        dangerous_keywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 'TRUNCATE']
        for keyword in dangerous_keywords:
            if keyword in sql_upper and f' {keyword} ' in f' {sql_upper} ':
                return False, f"检测到危险操作: {keyword}"

        # 检查必需的关键字
        required_keywords = ['SELECT', 'FROM']
        for keyword in required_keywords:
            if keyword not in sql_upper:
                return False, f"缺少必需关键字: {keyword}"

        return True, "语法正确"

    def generate_safe_sql(self, table_name: str, schema: List[Dict]) -> str:
        """生成安全的SQL查询"""
        if not schema:
            return f"SELECT 1 FROM `{table_name}` LIMIT 1"

        # 选择前5个字段
        columns = schema[:5]
        column_parts = []

        for col in columns:
            col_name = col['name']
            # 添加有意义的别名
            alias = self._get_column_alias(col_name)
            column_parts.append(f"`{col_name}` AS `{alias}`")

        columns_str = ", ".join(column_parts) if column_parts else "*"
        return f"SELECT {columns_str} FROM `{table_name}` LIMIT 10"

    def _get_column_alias(self, column_name: str) -> str:
        """获取列的中文别名"""
        col_lower = column_name.lower()

        mappings = {
            'id': 'ID', 'name': '名称', 'title': '标题', 'desc': '描述', 'description': '描述',
            'date': '日期', 'time': '时间', 'datetime': '日期时间', 'create_time': '创建时间',
            'update_time': '更新时间', 'amount': '金额', 'price': '价格', 'cost': '成本',
            'fee': '费用', 'money': '金额', 'total': '总计', 'sum': '合计', 'count': '数量',
            'quantity': '数量', 'qty': '数量', 'number': '编号', 'num': '编号', 'status': '状态',
            'state': '状态', 'type': '类型', 'category': '分类', 'class': '类别', 'group': '分组',
            'user': '用户', 'username': '用户名', 'password': '密码', 'email': '邮箱',
            'phone': '电话', 'mobile': '手机', 'address': '地址', 'city': '城市',
            'province': '省份', 'country': '国家', 'region': '区域', 'area': '地区',
            'score': '分数', 'grade': '等级', 'level': '级别', 'rate': '比率', 'ratio': '比例',
            'percent': '百分比', 'percentage': '百分比', 'age': '年龄', 'gender': '性别',
        }

        return mappings.get(col_lower, column_name)
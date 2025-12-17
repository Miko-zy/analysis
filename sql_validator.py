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

        # 查找没有反引号的列名
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql_no_strings, re.IGNORECASE | re.DOTALL)
        if select_match:
            select_clause = select_match.group(1)
            select_clause = re.sub(r'\w+\([^)]*\)', '', select_clause)
            words = re.findall(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b', select_clause)
            for word in words:
                if word.upper() not in self.keyword_patterns['aggregation'] and not word.isdigit():
                    columns.append(word)

        return list(set(columns))

    def extract_tables_from_sql(self, sql: str) -> List[str]:
        """从SQL中提取表名"""
        tables = []

        # 查找FROM子句中的表名
        from_match = re.search(r'FROM\s+([^,(]+)', sql, re.IGNORECASE)
        if from_match:
            table_str = from_match.group(1).strip()
            table_str = re.split(r'\s+(?:AS\s+)?\b[a-zA-Z_]\w*\b', table_str)[0].strip()
            table_name = table_str.replace('`', '')
            if table_name:
                tables.append(table_name)

        # 查找JOIN子句中的表名
        join_matches = re.findall(r'(?:JOIN|LEFT JOIN|INNER JOIN|RIGHT JOIN)\s+([^,\s]+)', sql, re.IGNORECASE)
        for match in join_matches:
            table_str = match.strip()
            table_str = re.split(r'\s+(?:AS\s+)?\b[a-zA-Z_]\w*\b', table_str)[0].strip()
            table_name = table_str.replace('`', '')
            if table_name:
                tables.append(table_name)

        return list(set(tables))

    def find_similar_column(self, invalid_column: str, available_columns: List[str]) -> Optional[str]:
        """找到最相似的列名"""
        if not available_columns:
            return None

        # 精确匹配（忽略大小写）
        for col in available_columns:
            if col.lower() == invalid_column.lower():
                return col

        # 部分匹配
        for col in available_columns:
            if invalid_column.lower() in col.lower() or col.lower() in invalid_column.lower():
                return col

        # 使用difflib找到最相似的
        matches = difflib.get_close_matches(invalid_column, available_columns, n=1, cutoff=0.6)
        if matches:
            return matches[0]

        # 常见列名映射
        column_mappings = {
            '订购数量': ['数量', 'quantity', 'qty', 'order_quantity', 'total_quantity'],
            '购买数量': ['数量', 'quantity', 'qty', 'purchase_quantity'],
            '销售数量': ['数量', 'quantity', 'qty', 'sales_quantity'],
            '订购金额': ['金额', 'amount', 'total_amount', 'order_amount', 'price'],
            '销售金额': ['金额', 'amount', 'sales_amount', 'revenue'],
            '购买金额': ['金额', 'amount', 'purchase_amount'],
            '订购时间': ['时间', 'date', 'datetime', 'order_date', 'created_at'],
            '购买时间': ['时间', 'date', 'datetime', 'purchase_date', 'buy_date'],
            '销售时间': ['时间', 'date', 'datetime', 'sales_date', 'sale_date'],
            '订单状态': ['状态', 'status', 'order_status'],
            '支付状态': ['状态', 'status', 'payment_status'],
            '发货状态': ['状态', 'status', 'delivery_status'],
        }

        if invalid_column in column_mappings:
            for possible_name in column_mappings[invalid_column]:
                if possible_name in available_columns:
                    return possible_name

        return None

    def validate_sql_structure(self, sql: str) -> Tuple[bool, List[str]]:
        """验证SQL基本结构"""
        warnings = []

        sql_upper = sql.upper()

        # 检查是否以SELECT开头
        if not sql_upper.strip().startswith('SELECT'):
            warnings.append("SQL应以SELECT开头")

        # 检查是否包含FROM
        if 'FROM' not in sql_upper:
            warnings.append("缺少FROM子句")

        # 检查危险关键字
        dangerous_keywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 'TRUNCATE']
        for keyword in dangerous_keywords:
            if f' {keyword} ' in f' {sql_upper} ':
                warnings.append(f"检测到危险操作: {keyword}")

        # 检查括号匹配
        if sql.count('(') != sql.count(')'):
            warnings.append("括号不匹配")

        # 检查是否有LIMIT
        if 'LIMIT' not in sql_upper and 'SELECT' in sql_upper:
            warnings.append("建议添加LIMIT子句限制返回行数")

        return len(warnings) == 0, warnings

    def fix_column_names(self, sql: str, available_columns: List[str], table_name: str = None) -> Tuple[
        str, Dict[str, str]]:
        """修正SQL中的列名"""
        fixed_sql = sql
        corrections = {}

        # 提取SQL中的列名
        columns_in_sql = self.extract_columns_from_sql(sql)

        for column in columns_in_sql:
            if column not in available_columns:
                similar_column = self.find_similar_column(column, available_columns)

                if similar_column:
                    patterns = [
                        f'`{column}`',
                        f'\\b{column}\\b',
                    ]

                    for pattern in patterns:
                        fixed_sql = re.sub(
                            pattern,
                            f'`{similar_column}`',
                            fixed_sql,
                            flags=re.IGNORECASE
                        )

                    corrections[column] = similar_column

        return fixed_sql, corrections

    def generate_safe_sql(self, table_name: str, table_schema: List[Dict], limit: int = 50) -> str:
        """生成安全的SQL查询"""
        columns = [col['name'] for col in table_schema]

        # 选择3-5个有代表性的列
        selected_columns = []
        column_priority = []

        for col in table_schema:
            col_name = col['name']
            col_type = col.get('type', '').lower()

            if col.get('primary_key', False):
                column_priority.append((col_name, 100))
            elif any(keyword in col_name.lower() for keyword in ['name', 'title', 'desc']):
                column_priority.append((col_name, 90))
            elif any(keyword in col_name.lower() for keyword in ['date', 'time', 'create', 'update']):
                column_priority.append((col_name, 80))
            elif any(keyword in col_type for keyword in ['int', 'float', 'decimal', 'double']):
                column_priority.append((col_name, 70))
            else:
                column_priority.append((col_name, 60))

        column_priority.sort(key=lambda x: x[1], reverse=True)
        selected_columns = [col[0] for col in column_priority[:5]]

        if not selected_columns:
            selected_columns = columns[:3] if len(columns) >= 3 else columns

        select_clause = ', '.join([f'`{col}`' for col in selected_columns])
        sql = f"SELECT {select_clause} FROM `{table_name}` LIMIT {limit}"

        return sql
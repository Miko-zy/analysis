import requests
import json
import re
import time
import logging
import traceback
from typing import Dict, List, Any, Tuple, Optional
from config import DASHSCOPE_CONFIG
from sql_validator import SQLValidator
import dashscope

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SQLTemplateSystem:
    """SQL模板系统 - 内嵌版本"""

    TEMPLATES = {
        'basic_select': {
            'description': '基础查询',
            'sql_template': "SELECT {columns} FROM `{table}` {where} {group_by} {order_by} {limit}",
            'default_limit': 100
        },
        'summary_stats': {
            'description': '统计汇总',
            'sql_template': """
SELECT 
    COUNT(*) as 总记录数,
    {numeric_columns}
FROM `{table}`
{where}
            """,
            'numeric_columns_template': "AVG(`{column}`) as `{column}_平均值`, MAX(`{column}`) as `{column}_最大值`, MIN(`{column}`) as `{column}_最小值`"
        },
        'group_by_stats': {
            'description': '分组统计',
            'sql_template': """
SELECT 
    `{group_column}` as 分组,
    COUNT(*) as 记录数,
    {agg_columns}
FROM `{table}`
{where}
GROUP BY `{group_column}`
ORDER BY 记录数 DESC
{limit}
            """
        },
        'time_series': {
            'description': '时间序列分析',
            'sql_template': """
SELECT 
    DATE(`{date_column}`) as 日期,
    COUNT(*) as 记录数,
    {agg_columns}
FROM `{table}`
WHERE `{date_column}` IS NOT NULL
GROUP BY DATE(`{date_column}`)
ORDER BY 日期
{limit}
            """
        },
        'ranking': {
            'description': '排名查询',
            'sql_template': """
SELECT 
    `{ranking_column}` as 名称,
    `{value_column}` as 数值
FROM `{table}`
WHERE `{value_column}` IS NOT NULL
ORDER BY `{value_column}` DESC
{limit}
            """
        },
        'related_query': {
            'description': '关联查询',
            'sql_template': """
SELECT 
    t1.`{t1_column}` as 表1字段,
    t2.`{t2_column}` as 表2字段,
    COUNT(*) as 关联数
FROM `{table1}` t1
JOIN `{table2}` t2 ON t1.`{join_key1}` = t2.`{join_key2}`
{where}
GROUP BY t1.`{t1_column}`, t2.`{t2_column}`
ORDER BY 关联数 DESC
{limit}
            """
        }
    }

    @staticmethod
    def classify_query_intent(natural_language_query: str) -> Tuple[str, Dict[str, Any]]:
        """分类查询意图"""
        query_lower = natural_language_query.lower()

        keyword_patterns = {
            'summary_stats': ['总计', '合计', '汇总', '总和', '平均', '最大值', '最小值', '统计', '数量'],
            'group_by_stats': ['分组', '分类', '按.*统计', '各个.*的', '每种', '每类', '各地区'],
            'time_series': ['时间', '日期', '月份', '季度', '年份', '趋势', '每天', '每月', '逐年'],
            'ranking': ['排名', '前.*名', '最高', '最低', '最多', '最少', '最好', '最差', 'top'],
            'related_query': ['关联', '关系', '连接', '涉及', '和.*一起', '同时'],
            'basic_select': ['查询', '查看', '显示', '列出', '找', '搜索']
        }

        matched_intent = 'basic_select'
        confidence = 0.0

        for intent, patterns in keyword_patterns.items():
            for pattern in patterns:
                if re.search(pattern, query_lower):
                    if intent != matched_intent:
                        matched_intent = intent
                        confidence = 0.8
                        break

        extracted_params = SQLTemplateSystem._extract_query_params(natural_language_query)

        return matched_intent, {
            'intent': matched_intent,
            'confidence': confidence,
            'extracted_params': extracted_params
        }

    @staticmethod
    def _extract_query_params(query: str) -> Dict[str, Any]:
        """从自然语言查询中提取参数"""
        params = {
            'table_names': [],
            'column_names': [],
            'numeric_columns': [],
            'date_columns': [],
            'text_columns': [],
            'filters': [],
            'sort_order': 'DESC',
            'limit_value': 10
        }

        table_patterns = ['表\s*[："\']?([^"\'，,。\.\s]+)']
        for pattern in table_patterns:
            matches = re.findall(pattern, query, re.IGNORECASE)
            if matches:
                params['table_names'].extend(matches)

        column_patterns = ['字段\s*[："\']?([^"\'，,。\.\s]+)', '列\s*[："\']?([^"\'，,。\.\s]+)']
        for pattern in column_patterns:
            matches = re.findall(pattern, query, re.IGNORECASE)
            if matches:
                params['column_names'].extend(matches)

        limit_matches = re.findall(r'前\s*(\d+)\s*名', query)
        if limit_matches:
            params['limit_value'] = int(limit_matches[0])
        elif re.search(r'前十|前20|前50', query):
            num_match = re.search(r'前(\d+)', query)
            if num_match:
                params['limit_value'] = int(num_match.group(1))

        if re.search(r'最低|最少|最小|降序|倒序', query):
            params['sort_order'] = 'ASC'

        return params

    @staticmethod
    def generate_from_template(
            intent: str,
            table_info: Dict[str, List[Dict]],
            extracted_params: Dict[str, Any]
    ) -> str:
        """根据模板生成SQL"""
        if intent not in SQLTemplateSystem.TEMPLATES:
            intent = 'basic_select'

        template = SQLTemplateSystem.TEMPLATES[intent]
        sql_template = template['sql_template']

        tables = list(table_info.keys())
        if not tables:
            return "SELECT 1"

        selected_table = tables[0]
        columns = table_info[selected_table]

        numeric_cols = [col['name'] for col in columns if col.get('category') in ['integer', 'numeric']]
        text_cols = [col['name'] for col in columns if col.get('category') == 'text']
        date_cols = [col['name'] for col in columns if col.get('category') == 'datetime']

        template_params = {
            'table': selected_table,
            'limit': f"LIMIT {extracted_params.get('limit_value', 100)}"
        }

        if intent == 'basic_select':
            select_cols = []
            for col in columns[:5]:
                col_name = col['name']
                alias = SQLTemplateSystem._get_column_alias(col_name)
                select_cols.append(f"`{col_name}` as `{alias}`")

            template_params['columns'] = ', '.join(select_cols) if select_cols else '*'
            template_params['where'] = ''
            template_params['group_by'] = ''
            template_params['order_by'] = 'ORDER BY 1'

        elif intent == 'summary_stats':
            if numeric_cols:
                numeric_templates = []
                for col in numeric_cols[:3]:
                    template_str = template['numeric_columns_template']
                    numeric_templates.append(template_str.format(column=col))

                template_params['numeric_columns'] = ', '.join(numeric_templates)
            else:
                template_params['numeric_columns'] = 'NULL as 无数值列'

        elif intent == 'group_by_stats':
            if text_cols:
                template_params['group_column'] = text_cols[0]
            elif columns:
                template_params['group_column'] = columns[0]['name']
            else:
                template_params['group_column'] = 'id'

            if numeric_cols:
                agg_columns = []
                for col in numeric_cols[:2]:
                    agg_columns.append(f"SUM(`{col}`) as `{col}_总和`, AVG(`{col}`) as `{col}_平均`")
                template_params['agg_columns'] = ', '.join(agg_columns)
            else:
                template_params['agg_columns'] = 'NULL as 无数值列'

        elif intent == 'time_series':
            if date_cols:
                template_params['date_column'] = date_cols[0]
            else:
                return SQLTemplateSystem.generate_from_template('group_by_stats', table_info, extracted_params)

            if numeric_cols:
                agg_columns = []
                for col in numeric_cols[:2]:
                    agg_columns.append(f"SUM(`{col}`) as `{col}_总计`")
                template_params['agg_columns'] = ', '.join(agg_columns)
            else:
                template_params['agg_columns'] = 'COUNT(*) as 记录数'

        elif intent == 'ranking':
            if numeric_cols and text_cols:
                template_params['ranking_column'] = text_cols[0]
                template_params['value_column'] = numeric_cols[0]
            elif numeric_cols and columns:
                template_params['ranking_column'] = columns[0]['name']
                template_params['value_column'] = numeric_cols[0]
            else:
                return SQLTemplateSystem.generate_from_template('basic_select', table_info, extracted_params)

        elif intent == 'related_query':
            if len(tables) >= 2:
                template_params['table1'] = tables[0]
                template_params['table2'] = tables[1]

                table1_cols = [col['name'] for col in table_info[tables[0]]]
                table2_cols = [col['name'] for col in table_info[tables[1]]]

                join_keys = set(table1_cols).intersection(set(table2_cols))
                if join_keys:
                    join_key = list(join_keys)[0]
                    template_params['join_key1'] = join_key
                    template_params['join_key2'] = join_key
                else:
                    template_params['join_key1'] = table1_cols[0] if table1_cols else 'id'
                    template_params['join_key2'] = table2_cols[0] if table2_cols else 'id'

                template_params['t1_column'] = table1_cols[0] if table1_cols else 'id'
                template_params['t2_column'] = table2_cols[0] if table2_cols else 'id'
            else:
                return SQLTemplateSystem.generate_from_template('group_by_stats', table_info, extracted_params)

        try:
            sql = sql_template.format(**template_params)
            sql = re.sub(r'\n\s*\n', '\n', sql)
            return sql.strip()
        except Exception as e:
            return f"SELECT * FROM `{selected_table}` LIMIT 10"

    @staticmethod
    def _get_column_alias(column_name: str) -> str:
        """获取列的中文别名"""
        col_lower = column_name.lower()

        english_mappings = {
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

        chinese_mappings = {
            'id': 'ID', '名称': '名称', '标题': '标题', '描述': '描述', '详情': '详情',
            '日期': '日期', '时间': '时间', '创建时间': '创建时间', '更新时间': '更新时间',
            '金额': '金额', '价格': '价格', '成本': '成本', '费用': '费用', '总数': '总数',
            '总计': '总计', '合计': '合计', '数量': '数量', '数目': '数目', '编号': '编号',
            '状态': '状态', '类型': '类型', '分类': '分类', '类别': '类别', '分组': '分组',
            '用户': '用户', '用户名': '用户名', '密码': '密码', '邮箱': '邮箱', '电话': '电话',
            '手机': '手机', '地址': '地址', '城市': '城市', '省份': '省份', '国家': '国家',
            '区域': '区域', '地区': '地区', '分数': '分数', '等级': '等级', '级别': '级别',
            '比率': '比率', '比例': '比例', '百分比': '百分比', '年龄': '年龄', '性别': '性别',
        }

        # 尝试英文匹配
        if col_lower in english_mappings:
            return english_mappings[col_lower]

        # 尝试中文匹配
        if column_name in chinese_mappings:
            return chinese_mappings[column_name]

        # 默认返回原名称
        return column_name


class LLMAnalyst:
    def __init__(self):
        self.api_key = DASHSCOPE_CONFIG['api_key']
        self.model = DASHSCOPE_CONFIG['model']
        self.max_retries = 3
        self.retry_delay = 2
        self.template_system = SQLTemplateSystem()
        self.sql_validator = SQLValidator()
        self.use_template_first = True
        self.auto_fix_columns = True

        # 设置dashscope api key
        dashscope.api_key = self.api_key

    def _call_dashscope(self, prompt, system_prompt=None, temperature=0.1):
        """调用阿里百炼API"""
        for attempt in range(self.max_retries):
            try:
                messages = []
                if system_prompt:
                    messages.append({"role": "system", "content": system_prompt})
                messages.append({"role": "user", "content": prompt})

                response = dashscope.Generation.call(
                    model=self.model,
                    messages=messages,
                    temperature=temperature,
                    top_p=0.9,
                    top_k=40,
                    max_tokens=2000,
                    result_format='message'
                )

                if response.status_code != 200:
                    error_msg = f"DashScope API错误: 状态码 {response.status_code}, 响应: {response}"
                    logger.error(error_msg)
                    if attempt == self.max_retries - 1:
                        return error_msg
                    time.sleep(self.retry_delay)
                    continue

                if response.output and response.output.choices:
                    content = response.output.choices[0].message.content.strip()
                    logger.info(f"DashScope响应成功，长度: {len(content)} 字符")
                    return content
                else:
                    error_msg = f"响应格式错误: {response}"
                    logger.error(error_msg)
                    return error_msg

            except Exception as e:
                error_msg = f"调用DashScope API时发生错误: {str(e)}"
                logger.error(error_msg)
                if attempt == self.max_retries - 1:
                    return error_msg
                time.sleep(self.retry_delay)

        return "请求失败"

    def _build_detailed_schema_info(self, table_schemas: Dict[str, List[Dict]]) -> str:
        """构建详细的表结构信息"""
        schema_info = ""

        for table_name, columns in table_schemas.items():
            primary_keys = [col['name'] for col in columns if col.get('primary_key', False)]
            numeric_cols = [col['name'] for col in columns if col.get('category') in ['integer', 'numeric']]
            text_cols = [col['name'] for col in columns if col.get('category') == 'text']
            date_cols = [col['name'] for col in columns if col.get('category') == 'datetime']

            schema_info += f"【表: {table_name}】\n"

            if primary_keys:
                schema_info += f"  主键: {', '.join(primary_keys)}\n"

            schema_info += f"  字段列表 ({len(columns)} 个):\n"

            for i, col in enumerate(columns[:10], 1):
                col_type = str(col.get('type', '')).upper()
                nullable = "可空" if col.get('nullable', True) else "非空"
                pk = "主键" if col.get('primary_key', False) else ""

                col_name = col['name'].lower()
                inferred_type = ""

                if any(x in col_name for x in ['id', 'code', 'no', 'num']):
                    inferred_type = "标识符"
                elif any(x in col_name for x in ['name', 'title', 'desc', 'note']):
                    inferred_type = "名称/描述"
                elif any(x in col_name for x in ['date', 'time', 'year', 'month', 'day']):
                    inferred_type = "日期时间"
                elif any(x in col_name for x in ['amount', 'price', 'cost', 'fee', 'money']):
                    inferred_type = "金额"
                elif any(x in col_name for x in ['count', 'quantity', 'qty', 'number']):
                    inferred_type = "数量"
                elif any(x in col_name for x in ['rate', 'ratio', 'percent']):
                    inferred_type = "比率"
                elif any(x in col_name for x in ['status', 'type', 'category', 'class']):
                    inferred_type = "分类"
                elif any(x in col_name for x in ['phone', 'email', 'address']):
                    inferred_type = "联系方式"

                if inferred_type:
                    inferred_type = f" ({inferred_type})"

                schema_info += f"  {i}. `{col['name']}` {col_type} {nullable} {pk}{inferred_type}\n"

            schema_info += f"  统计: {len(numeric_cols)}个数值字段, {len(text_cols)}个文本字段, {len(date_cols)}个日期字段\n\n"

        return schema_info

    def generate_sql_query(self, natural_language_query: str, table_schemas: Dict[str, List[Dict]]) -> str:
        """根据自然语言生成SQL查询"""
        if not natural_language_query.strip():
            return "请输入查询问题"

        try:
            logger.info(f"为自然语言查询生成SQL: {natural_language_query}")

            # 阶段1：使用模板系统生成SQL
            template_sql = self._generate_sql_from_template(natural_language_query, table_schemas)

            # 验证模板SQL
            if self.auto_fix_columns:
                template_sql = self._validate_and_fix_sql(template_sql, table_schemas)

            # 阶段2：如果配置了API KEY，尝试优化
            if self.api_key and self.api_key != "":
                try:
                    # 使用大模型优化模板SQL
                    optimized_sql = self._optimize_sql_with_llm(
                        natural_language_query,
                        table_schemas,
                        template_sql
                    )

                    # 验证并修正优化后的SQL
                    if self.auto_fix_columns:
                        optimized_sql = self._validate_and_fix_sql(optimized_sql, table_schemas)

                    # 验证优化后的SQL
                    if self._validate_sql_quality(optimized_sql, template_sql):
                        logger.info("使用大模型优化的SQL")
                        return optimized_sql
                    else:
                        logger.warning("大模型优化的SQL质量不佳，使用模板SQL")
                        return template_sql

                except Exception as e:
                    logger.warning(f"大模型优化失败，使用模板SQL: {e}")
                    return template_sql
            else:
                # 没有配置大模型，直接使用模板SQL
                logger.info("使用模板系统生成的SQL")
                return template_sql

        except Exception as e:
            error_msg = f"生成SQL时出现异常: {str(e)}"
            logger.error(error_msg)
            traceback.print_exc()

            # 返回安全的回退SQL
            fallback_sql = self._generate_safe_fallback_sql(table_schemas)
            return f"-- 错误: {error_msg}\n{fallback_sql}"

    def _generate_sql_from_template(self, natural_language_query: str, table_schemas: Dict[str, List[Dict]]) -> str:
        """使用模板系统生成SQL"""
        # 分类查询意图
        intent, intent_info = SQLTemplateSystem.classify_query_intent(natural_language_query)
        logger.info(f"识别查询意图: {intent} (置信度: {intent_info['confidence']})")

        # 提取查询参数
        extracted_params = intent_info['extracted_params']

        # 生成SQL
        sql_query = SQLTemplateSystem.generate_from_template(intent, table_schemas, extracted_params)

        return sql_query

    def _validate_and_fix_sql(self, sql: str, table_schemas: Dict[str, List[Dict]]) -> str:
        """验证并修正SQL"""
        try:
            if not sql or not isinstance(sql, str):
                return sql

            # 1. 移除多余的空白字符
            sql = re.sub(r'\s+', ' ', sql).strip()

            # 2. 提取表名
            table_pattern = r'FROM\s+`?([^`\s,]+)`?'
            tables = re.findall(table_pattern, sql, re.IGNORECASE)

            # 3. 为每个表修正列名
            for table_name in tables:
                if table_name in table_schemas:
                    available_columns = [col['name'] for col in table_schemas[table_name]]
                    # 修正列名
                    fixed_sql, corrections = self.sql_validator.fix_column_names(sql, available_columns, table_name)

                    if corrections:
                        logger.info(f"修正了列名: {corrections}")
                        sql = fixed_sql
                else:
                    logger.warning(f"表 '{table_name}' 不在数据库表结构中")

            # 4. 确保有LIMIT
            if 'LIMIT' not in sql.upper() and 'SELECT' in sql.upper():
                if 'ORDER BY' in sql.upper():
                    sql = re.sub(
                        r'(ORDER BY.*?)(?=$|;)',
                        r'\1 LIMIT 50',
                        sql,
                        flags=re.IGNORECASE | re.DOTALL
                    )
                else:
                    sql += ' LIMIT 50'

            return sql

        except Exception as e:
            logger.error(f"验证修正SQL失败: {e}")
            return sql

    def _generate_safe_fallback_sql(self, table_schemas: Dict[str, List[Dict]]) -> str:
        """生成安全的回退SQL"""
        tables = list(table_schemas.keys())
        if not tables:
            return "SELECT 1"

        table_name = tables[0]
        schema = table_schemas[table_name]

        return self.sql_validator.generate_safe_sql(table_name, schema)

    def _optimize_sql_with_llm(self, natural_language_query: str, table_schemas: Dict, template_sql: str) -> str:
        """使用大模型优化SQL"""
        # 构建详细的schema信息
        schema_info = self._build_detailed_schema_info(table_schemas)

        prompt = f"""
你是一个SQL专家。请根据用户需求优化以下SQL查询。

用户需求: {natural_language_query}

数据库表结构:
{schema_info}

现有SQL模板（仅供参考）:
{template_sql}

请优化这个SQL查询，确保：
1. 准确反映用户需求
2. 语法正确
3. 使用正确的表名和列名（用反引号包裹）
4. 包含适当的LIMIT子句
5. 添加有意义的列别名
6. 使用的表名和字符串值必须来源于数据库

请直接返回优化后的SQL语句（只返回SQL，不要有其他内容）:
"""

        system_prompt = """你是一个SQL优化专家。请直接返回优化后的SQL语句，不要包含任何解释、注释或其他文本。"""

        sql_query = self._call_dashscope(prompt, system_prompt, temperature=0.1)

        # 清理响应
        sql_query = sql_query.strip()
        sql_query = re.sub(r'```(?:sql)?|```', '', sql_query).strip()

        # 提取SQL语句
        sql_match = re.search(r'(SELECT\s+.*?)(?=;|$)', sql_query, re.IGNORECASE | re.DOTALL)
        if sql_match:
            sql_query = sql_match.group(1).strip()

        # 确保有SELECT
        if not sql_query.upper().startswith('SELECT'):
            return template_sql

        return sql_query

    def _validate_sql_quality(self, llm_sql: str, template_sql: str) -> bool:
        """验证SQL质量"""
        # 基本检查
        if not llm_sql or len(llm_sql.strip()) < 10:
            return False

        # 必须包含SELECT
        if 'SELECT' not in llm_sql.upper():
            return False

        # 必须包含FROM
        if 'FROM' not in llm_sql.upper():
            return False

        # 长度不能太短（相对模板SQL）
        if len(llm_sql) < len(template_sql) * 0.5:
            return False

        # 不能包含危险关键字
        dangerous_keywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 'TRUNCATE']
        for keyword in dangerous_keywords:
            if keyword in llm_sql.upper() and f' {keyword} ' in f' {llm_sql.upper()} ':
                return False

        return True

    def check_ollama_connection(self) -> Tuple[bool, List[str]]:
        """检查DashScope连接和模型可用性"""
        try:
            # 对于DashScope，我们只需要检查API Key是否有效
            if not self.api_key or self.api_key == "":
                return False, ["未配置API Key"]

            # 尝试调用一个简单的API来验证密钥
            response = dashscope.Models.list()
            if response.status_code == 200:
                logger.info("DashScope连接成功")
                return True, [self.model]
            else:
                error_msg = f"DashScope API错误: {response}"
                logger.error(error_msg)
                return False, [error_msg]
        except Exception as e:
            error_msg = f"连接错误: {str(e)}"
            logger.error(error_msg)
            return False, [error_msg]

    def validate_sql_query(self, sql_query):
        """验证SQL查询的合理性"""
        if not sql_query or not isinstance(sql_query, str):
            return False, "SQL查询不能为空"

        sql_upper = sql_query.upper().strip()

        # 基础安全检查
        dangerous_keywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 'TRUNCATE']
        for keyword in dangerous_keywords:
            if keyword in sql_upper and f' {keyword} ' in f' {sql_upper} ':
                return False, f"检测到不允许的操作: {keyword}"

        # 必须是以SELECT开头
        if not sql_upper.startswith('SELECT'):
            return False, "只支持SELECT查询"

        return True, "SQL查询格式正确"

    def improve_sql_with_feedback(self, original_sql: str, user_feedback: str,
                                  table_schemas: Dict[str, List[Dict]],
                                  original_query: str = None) -> str:
        """根据用户反馈改进SQL查询"""
        if not original_sql.strip():
            return "原始SQL为空"

        if not user_feedback.strip():
            return original_sql

        try:
            # 1. 首先尝试自动修正
            fixed_sql = self._validate_and_fix_sql(original_sql, table_schemas)

            # 如果只是列名错误，直接返回修正后的SQL
            if "Unknown column" in user_feedback and fixed_sql != original_sql:
                logger.info("已自动修正列名错误")
                return f"-- 自动修正列名错误\n{fixed_sql}"

            # 2. 使用大模型改进
            return self._improve_sql_with_llm_feedback(original_sql, user_feedback, table_schemas, original_query)

        except Exception as e:
            error_msg = f"改进SQL时出现异常: {str(e)}"
            logger.error(error_msg)
            return original_sql

    def _improve_sql_with_llm_feedback(self, original_sql: str, user_feedback: str,
                                       table_schemas: Dict[str, List[Dict]],
                                       original_query: str = None) -> str:
        """使用大模型改进SQL"""
        # 构建详细的schema信息
        schema_info = self._build_detailed_schema_info(table_schemas)

        from config import DATABASE_CONFIG
        db_dialect = DATABASE_CONFIG['dialect']

        prompt = f"""
你是一个SQL专家。请根据用户的反馈改进以下SQL查询。

原始查询需求: {original_query if original_query else "未提供原始需求"}

数据库表结构:
{schema_info}

原始SQL:
{original_sql}

用户反馈: {user_feedback}

请改进这个SQL查询，确保：
1. 完全理解并满足用户反馈的要求
2. 只使用数据库中存在的表和字段名称
3. 如果字段名不存在，使用最相似的字段名
4. 保持SQL语法正确性，适用于{db_dialect.upper()}数据库
5. 使用反引号(`)包裹表名和列名
6. 必须包含LIMIT子句（建议LIMIT 50）
7. 保持查询简单直接

请直接返回改进后的SQL语句（只返回SQL，不要有其他内容）:
"""

        system_prompt = f"""你是一个SQL优化专家。请根据用户反馈直接返回改进后的SQL语句。
确保：
1. 只返回SQL，不要有其他内容
2. 只使用存在的表和字段
3. 必须包含LIMIT
4. SQL语法正确"""

        logger.info("使用大模型改进SQL...")
        improved_sql = self._call_dashscope(prompt, system_prompt, temperature=0.2)

        # 清理响应
        improved_sql = improved_sql.strip()
        improved_sql = re.sub(r'```(?:sql)?|```', '', improved_sql).strip()

        # 提取SQL语句
        sql_match = re.search(r'(SELECT\s+.*?)(?=;|$)', improved_sql, re.IGNORECASE | re.DOTALL)
        if sql_match:
            improved_sql = sql_match.group(1).strip()

        # 验证并修正
        improved_sql = self._validate_and_fix_sql(improved_sql, table_schemas)

        # 添加反馈注释
        commented_sql = f"-- 根据用户反馈改进的SQL\n-- 反馈: {user_feedback[:100]}\n{improved_sql}"

        logger.info("SQL改进成功")
        return commented_sql

    def analyze_data_insights(self, analysis_request: str, data_description: str) -> str:
        """使用AI分析数据并提供洞察"""
        try:
            prompt = f"""
作为数据分析专家，请根据用户请求分析以下数据并提供有价值的洞察。

用户分析请求: {analysis_request}

数据描述:
{data_description}

请提供以下方面的分析:
1. 数据概览和关键发现
2. 重要的趋势和模式
3. 异常值或值得注意的现象
4. 业务含义和潜在机会
5. 建议的后续行动或深入分析方向

请用中文回答，保持专业性和实用性。
"""

            system_prompt = "你是一位经验丰富的数据分析师，擅长从数据中发现有价值的洞察并提供实用的业务建议。"

            insights = self._call_dashscope(prompt, system_prompt, temperature=0.3)
            return insights

        except Exception as e:
            error_msg = f"AI数据分析失败: {str(e)}"
            logger.error(error_msg)
            return error_msg

    def call_ollama_api(self, prompt: str, temperature: float = 0.1) -> str:
        """调用大模型API的通用方法（兼容旧接口）"""
        return self._call_dashscope(prompt, temperature=temperature)
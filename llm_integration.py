import requests
import json
import re
import time
import logging
import traceback
from typing import Dict, List, Any, Tuple, Optional
from config import OLLAMA_CONFIG
from sql_validator import SQLValidator

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
            'sex': '性别', 'birth': '生日', 'birthday': '生日'
        }

        if re.search(r'[\u4e00-\u9fff]', column_name):
            return column_name

        for eng_key, chi_value in english_mappings.items():
            if eng_key == col_lower or f'_{eng_key}' in col_lower or f'{eng_key}_' in col_lower:
                return chi_value

        suffixes = {
            '_id': 'ID', '_name': '名称', '_time': '时间', '_date': '日期',
            '_amount': '金额', '_price': '价格', '_count': '数量', '_total': '总计',
            '_status': '状态', '_type': '类型'
        }

        for suffix, alias in suffixes.items():
            if col_lower.endswith(suffix):
                prefix = col_lower[:-len(suffix)]
                if prefix in english_mappings:
                    return f"{english_mappings[prefix]}{alias}"
                else:
                    return f"{prefix}{alias}"

        return column_name


class LLMAnalyst:
    def __init__(self):
        self.base_url = OLLAMA_CONFIG['base_url']
        self.model = OLLAMA_CONFIG['model']
        self.timeout = OLLAMA_CONFIG['timeout']
        self.max_retries = 3
        self.retry_delay = 2
        self.template_system = SQLTemplateSystem()
        self.sql_validator = SQLValidator()
        self.use_template_first = True
        self.auto_fix_columns = True

    def _call_ollama(self, prompt, system_prompt=None, temperature=0.1):
        """调用Ollama API"""
        for attempt in range(self.max_retries):
            try:
                messages = []
                if system_prompt:
                    messages.append({"role": "system", "content": system_prompt})
                messages.append({"role": "user", "content": prompt})

                payload = {
                    "model": self.model,
                    "messages": messages,
                    "stream": False,
                    "options": {
                        "temperature": temperature,
                        "top_p": 0.9,
                        "top_k": 40,
                        "num_predict": 6000
                    }
                }

                response = requests.post(
                    f"{self.base_url}/api/chat",
                    json=payload,
                    timeout=self.timeout
                )

                if response.status_code != 200:
                    error_msg = f"Ollama API错误: 状态码 {response.status_code}, 响应: {response.text}"
                    logger.error(error_msg)
                    if attempt == self.max_retries - 1:
                        return error_msg
                    time.sleep(self.retry_delay)
                    continue

                result = response.json()

                if 'message' in result and 'content' in result['message']:
                    content = result['message']['content'].strip()
                    logger.info(f"Ollama响应成功，长度: {len(content)} 字符")
                    return content
                else:
                    error_msg = f"响应格式错误: {result}"
                    logger.error(error_msg)
                    return error_msg

            except requests.exceptions.ConnectionError:
                error_msg = f"无法连接到Ollama服务，请确保Ollama正在运行在 {self.base_url}"
                logger.error(error_msg)
                if attempt == self.max_retries - 1:
                    return error_msg
                time.sleep(self.retry_delay)

            except requests.exceptions.Timeout:
                error_msg = "请求超时，请检查Ollama服务状态"
                logger.error(error_msg)
                if attempt == self.max_retries - 1:
                    return error_msg
                time.sleep(self.retry_delay)

            except Exception as e:
                error_msg = f"未知错误: {str(e)}"
                logger.error(error_msg)
                return error_msg

        return "所有重试尝试都失败了"

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

            # 阶段2：如果配置了Ollama且连接正常，尝试优化
            if self.base_url and self.model and self.base_url != "":
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
            logger.error(traceback.format_exc())
            # 返回安全的查询
            return self._generate_safe_fallback_sql(table_schemas)

    def _generate_sql_from_template(self, natural_language_query: str, table_schemas: Dict) -> str:
        """使用模板系统生成SQL"""
        try:
            intent, intent_info = self.template_system.classify_query_intent(natural_language_query)
            logger.info(f"查询意图分类: {intent}, 置信度: {intent_info['confidence']}")

            template_sql = self.template_system.generate_from_template(
                intent,
                table_schemas,
                intent_info['extracted_params']
            )

            template_desc = self.template_system.TEMPLATES.get(intent, {}).get('description', '查询')
            commented_sql = f"-- 基于模板生成: {template_desc}\n{template_sql}"

            logger.info(f"模板生成SQL成功，意图: {intent}")
            return commented_sql

        except Exception as e:
            logger.error(f"模板系统生成SQL失败: {e}")
            logger.error(traceback.format_exc())
            tables = list(table_schemas.keys())
            if tables:
                return f"SELECT * FROM `{tables[0]}` LIMIT 10"
            return "SELECT 1"

    def _validate_and_fix_sql(self, sql: str, table_schemas: Dict[str, List[Dict]]) -> str:
        """验证并修正SQL"""
        try:
            # 1. 验证SQL结构
            is_valid, warnings = self.sql_validator.validate_sql_structure(sql)
            if warnings:
                logger.warning(f"SQL结构警告: {warnings}")

            # 2. 提取表名
            tables = self.sql_validator.extract_tables_from_sql(sql)

            if not tables:
                logger.warning("SQL中没有找到表名，生成安全查询")
                return self._generate_safe_fallback_sql(table_schemas)

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

        sql_query = self._call_ollama(prompt, system_prompt, temperature=0.1)

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

        # 检查危险操作
        dangerous = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 'TRUNCATE']
        for d in dangerous:
            if f' {d} ' in f' {llm_sql.upper()} ':
                return False

        # 检查是否过于简单
        if llm_sql.upper().strip() == 'SELECT 1':
            return False

        return True

    def analyze_data_insights(self, analysis_prompt, data_description, analysis_level="standard"):
        """使用大模型分析数据洞察，支持不同分析等级"""
        if not analysis_prompt.strip():
            return "请输入分析需求"

        if not data_description.strip():
            return "数据描述为空"

        # 分析等级配置
        analysis_levels = {
            "basic": {
                "name": "基础分析",
                "temperature": 0.3,
                "max_tokens": 1500,
                "instruction": "提供简洁的数据概览和主要发现"
            },
            "standard": {
                "name": "标准分析",
                "temperature": 0.5,
                "max_tokens": 2500,
                "instruction": "提供详细的数据分析、趋势洞察和业务建议"
            },
            "advanced": {
                "name": "深度分析",
                "temperature": 0.7,
                "max_tokens": 4000,
                "instruction": "提供全面的多维度分析、深度洞察和战略建议"
            },
            "expert": {
                "name": "专家级分析",
                "temperature": 0.8,
                "max_tokens": 6000,
                "instruction": "提供学术级的统计分析、预测建模和前沿洞察"
            }
        }

        level_config = analysis_levels.get(analysis_level, analysis_levels["standard"])

        # 构建分析提示
        prompt = self._build_analysis_prompt(analysis_prompt, data_description, level_config)

        system_prompt = self._build_system_prompt(level_config)

        logger.info(f"正在进行{level_config['name']}...")

        # 调用Ollama
        insights = self._call_ollama(
            prompt,
            system_prompt,
            temperature=level_config["temperature"]
        )

        # 添加分析等级信息
        formatted_insights = f"## 🔬 {level_config['name']}报告\n\n"
        formatted_insights += f"**分析等级**: {level_config['name']}\n"
        formatted_insights += f"**分析需求**: {analysis_prompt}\n\n"
        formatted_insights += insights

        return formatted_insights

    def _build_analysis_prompt(self, analysis_prompt, data_description, level_config):
        """构建分析提示"""

        level_name = level_config.get("name", "标准分析")

        if level_name == "基础分析":
            prompt = f"""
你是一个数据分析师。请对以下数据进行简洁分析：

分析需求: {analysis_prompt}

数据描述:
{data_description}

请提供：
1. 数据基本情况（3-4句话）
2. 主要发现（2-3个关键点）
3. 简要建议

保持回答简洁明了，不超过500字。
"""

        elif level_name == "标准分析":
            prompt = f"""
你是一个专业的数据分析师。请分析以下数据并提供深入的业务洞察：

分析需求: {analysis_prompt}

数据描述:
{data_description}

请用中文提供以下分析：
1. 📊 **数据概览** - 数据基本情况、数据质量和完整性评估
2. 🔍 **主要发现** - 3-4个最重要的发现和趋势
3. 📈 **深入分析** - 关键指标的变化趋势和模式识别
4. ⚠️ **异常检测** - 数据中的异常值或有趣模式
5. 💡 **业务建议** - 基于发现的实用建议和行动计划
6. 🔮 **后续分析方向** - 进一步分析的潜在方向

请用清晰的结构化格式回答，使用适当的标题和项目符号。
确保分析基于提供的数据，不要虚构不存在的信息。
"""

        elif level_name == "深度分析":
            prompt = f"""
你是一个资深的数据科学家。请对以下数据进行全面的多维度分析：

分析需求: {analysis_prompt}

数据描述:
{data_description}

请提供以下内容的详细分析：

## 🎯 **一、分析框架**
- 分析目标和方法论
- 数据预处理和质量评估
- 分析维度和指标体系

## 📊 **二、多维度数据分析**
### 1. 描述性统计分析
- 中心趋势度量（均值、中位数、众数）
- 离散程度度量（方差、标准差、范围）
- 分布形态（偏度、峰度、分布检验）

### 2. 趋势与时序分析
- 时间序列模式识别
- 季节性、周期性和趋势性分析
- 变化率和增长率计算

### 3. 相关性分析
- 变量间相关系数矩阵
- 显著性检验（p值）
- 因果关系初步推断

### 4. 分组与对比分析
- 不同维度的分组统计
- 方差分析和显著性差异
- 交互效应分析

## 🔍 **三、深度洞察**
### 1. 模式识别
- 数据中隐藏的模式和规律
- 异常值和离群点分析
- 聚类和分类模式

### 2. 预测性分析
- 基于现有数据的趋势预测
- 风险评估和概率估计
- 敏感性分析

### 3. 商业智能洞察
- KPI指标分解和解读
- ROI和效能评估
- 机会识别和风险评估

## 💡 **四、战略建议**
### 1. 立即行动建议
### 2. 中期优化策略
### 3. 长期战略规划
### 4. 风险防范措施

## 📋 **五、技术细节**
- 使用的分析方法说明
- 假设和局限性说明
- 数据质量改进建议

请确保分析专业、深入，并提供可执行的建议。
"""

        else:  # 专家级分析
            prompt = f"""
你是一个顶级的数据科学专家。请对以下数据进行学术级分析：

分析需求: {analysis_prompt}

数据描述:
{data_description}

请提供学术论文级别的分析报告，包括：

## 🏛️ **一、研究设计与方法论**
### 1. 研究问题与假设
- 研究问题的明确表述
- 理论框架和假设设定
- 研究范围和限制条件

### 2. 方法论设计
- 数据分析方法选择依据
- 统计模型构建和验证
- 信度和效度评估

## 📈 **二、高级统计分析**
### 1. 多元统计分析
- 主成分分析(PCA)和因子分析
- 聚类分析和判别分析
- 结构方程模型(SEM)

### 2. 预测建模
- 回归模型（线性、逻辑、多项式）
- 时间序列模型（ARIMA、ETS）
- 机器学习模型（随机森林、XGBoost）

### 3. 假设检验
- A/B测试设计和分析
- 方差分析(ANOVA)
- 非参数检验

## 🧠 **三、认知洞察**
### 1. 因果推断
- 因果图建模
- 倾向得分匹配
- 断点回归设计

### 2. 贝叶斯分析
- 贝叶斯统计推断
- 后验分布分析
- 马尔可夫链蒙特卡洛(MCMC)

### 3. 网络分析
- 社交网络分析
- 图论方法应用
- 复杂系统分析

## 📊 **四、可视化与报告**
### 1. 高级数据可视化
- 交互式可视化设计
- 多维数据展示技术
- 仪表板和报告设计

### 2. 结果解释
- 统计结果的业务解读
- 效应大小和实际意义
- 不确定性和置信区间

## 🎓 **五、学术贡献**
### 1. 理论贡献
### 2. 实践意义
### 3. 研究局限性
### 4. 未来研究方向

请提供严谨、深入的分析，包括统计检验、模型参数、假设验证等学术细节。
"""

        return prompt

    def _build_system_prompt(self, level_config):
        """构建系统提示"""

        level_name = level_config.get("name", "标准分析")

        if level_name == "基础分析":
            return """你是一个数据分析助手，擅长用简洁的语言总结数据的主要发现。
请用中文回答，重点突出，语言简洁。"""

        elif level_name == "标准分析":
            return """你是一个专业的数据分析师，擅长从数据中发现洞察并提供实用的业务建议。
请用中文回答，结构清晰，内容实用，基于实际数据进行分析。"""

        elif level_name == "深度分析":
            return """你是一个资深的数据科学家，具有多领域的数据分析经验。
你擅长使用统计方法、机器学习技术和商业智能工具进行深度分析。
请提供专业、深入、可操作的分析报告，使用技术术语但要确保可理解性。"""

        else:  # 专家级分析
            return """你是一个顶尖的数据科学专家，具有学术研究和行业应用的丰富经验。
你擅长使用先进的数据分析方法，能够提供学术论文级别的分析报告。
请保持分析的严谨性、深度和原创性，同时确保结果的实用价值。"""

    def analyze_data_multidimensional(self, data_description, dimensions=None):
        """多维度数据分析"""
        if dimensions is None:
            dimensions = ["时间", "地理", "产品", "客户", "渠道"]

        prompt = f"""
你是一个多维数据分析专家。请从以下维度对数据进行全面分析：

数据描述:
{data_description}

请从以下维度进行分析：
{', '.join(dimensions)}

对于每个维度，请提供：
1. **维度重要性** - 该维度对业务的关键程度
2. **数据分布** - 在该维度上的数据分布情况
3. **模式发现** - 在该维度上发现的模式和规律
4. **交叉分析** - 该维度与其他维度的交互关系
5. **维度建议** - 针对该维度的优化建议

最后，请提供：
- **维度重要性排序**
- **关键交叉维度组合**
- **多维分析行动计划**

请用中文回答，结构清晰，重点突出。
"""

        system_prompt = """你是一个多维数据分析专家，擅长从多个角度分析数据，发现隐藏的模式和关系。
请提供结构化的多维度分析报告。"""

        logger.info("正在进行多维度数据分析...")
        analysis = self._call_ollama(prompt, system_prompt, temperature=0.6)

        return f"## 🎯 多维度数据分析报告\n\n**分析维度**: {', '.join(dimensions)}\n\n{analysis}"

    def analyze_data_trends(self, data_description, time_period="所有时期"):
        """趋势分析专用方法"""
        prompt = f"""
你是一个趋势分析专家。请对以下数据进行趋势分析：

数据描述:
{data_description}

分析时期: {time_period}

请提供以下分析：

## 📈 **趋势分析报告**

### 一、整体趋势分析
1. **长期趋势** - 数据整体的上升/下降趋势
2. **变化速度** - 趋势变化的速率和加速度
3. **趋势稳定性** - 趋势的稳定性和波动性

### 二、周期性分析
1. **季节性模式** - 明显的季节性规律
2. **周期性波动** - 固定周期的波动模式
3. **随机波动** - 不可预测的随机变化

### 三、转折点分析
1. **趋势转折点** - 趋势发生改变的关键时点
2. **影响因素分析** - 可能导致转折的因素
3. **转折显著性** - 转折的统计显著性

### 四、预测分析
1. **短期预测** - 未来短期内的趋势预测
2. **中期展望** - 未来中期的趋势展望
3. **长期趋势判断** - 长期趋势的方向判断

### 五、业务影响评估
1. **机会识别** - 趋势带来的业务机会
2. **风险评估** - 趋势带来的潜在风险
3. **应对策略** - 针对趋势的应对策略

请提供具体的时间段、变化百分比和业务影响评估。
"""

        system_prompt = """你是一个趋势分析专家，擅长识别数据中的各种趋势模式，
并提供基于趋势的业务洞察和预测。请用中文回答。"""

        logger.info("正在进行趋势分析...")
        analysis = self._call_ollama(prompt, system_prompt, temperature=0.5)

        return f"## 📈 趋势分析报告\n\n**分析时期**: {time_period}\n\n{analysis}"

    def generate_executive_summary(self, full_analysis):
        """生成执行摘要"""
        prompt = f"""
请根据以下详细分析报告，生成一份简洁的执行摘要：

详细分析报告:
{full_analysis}

请生成包含以下内容的执行摘要：
1. 🎯 **核心发现** - 最重要的3个发现
2. ⚡ **关键指标** - 最重要的3个指标
3. 🚀 **立即行动** - 需要立即采取的3个行动
4. ⚠️ **主要风险** - 最主要的2个风险
5. 💡 **战略建议** - 最重要的2个战略建议

请用bullet point形式，语言简洁有力，适合管理层阅读。
"""

        system_prompt = """你是一个商业分析师，擅长从详细报告中提炼关键信息，
生成适合管理层阅读的执行摘要。"""

        logger.info("正在生成执行摘要...")
        summary = self._call_ollama(prompt, system_prompt, temperature=0.3)

        return f"## 📋 执行摘要\n\n{summary}"

    def check_ollama_connection(self):
        """检查Ollama连接和模型可用性"""
        try:
            response = requests.get(f"{self.base_url}/api/tags", timeout=10)
            if response.status_code == 200:
                data = response.json()
                if 'models' in data:
                    models = data['models']
                    available_models = [model['name'] for model in models]
                    logger.info(f"Ollama连接成功，可用模型: {available_models}")

                    if self.model not in available_models:
                        logger.warning(f"配置的模型 {self.model} 不可用，使用第一个可用模型")
                        if available_models:
                            self.model = available_models[0]
                            logger.info(f"切换到模型: {self.model}")

                    return True, available_models
                else:
                    error_msg = "API响应格式异常"
                    logger.error(error_msg)
                    return False, [error_msg]
            else:
                error_msg = f"HTTP错误: {response.status_code}"
                logger.error(error_msg)
                return False, [error_msg]
        except requests.exceptions.ConnectionError:
            error_msg = "无法连接到Ollama服务"
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
        improved_sql = self._call_ollama(prompt, system_prompt, temperature=0.2)

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